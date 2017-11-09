/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.plugin.fileappend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Append output format
 */
public class AppendOutputFormat extends OutputFormat<NullWritable, Text> {
  public static final String OUTPUT_DIR = "cask.append.output.dir";
  public static final String SIZE_THRESHOLD = "cask.append.size.threshold";
  public static final String AGE_THRESHOLD_SEC = "cask.append.age.threshold.sec";
  public static final String START_TIME = "cask.append.logical.start.time";
  public static final String FILE_PREFIX = "cask.append.file.prefix";
  public static final String IS_LOCAL_MODE = "cask.append.is.local.mode";
  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }

  @Override
  public RecordWriter<NullWritable, Text> getRecordWriter(TaskAttemptContext job)
    throws IOException, InterruptedException {

    Configuration conf = job.getConfiguration();

    FileSystem fs;
    // hack to workaround the fact that LocalFileSystem doesn't implement append()
    if (FsConstants.LOCAL_FS_URI.equals(FileSystem.getDefaultUri(conf))) {
      fs = new RawLocalFileSystem();
      fs.initialize(FsConstants.LOCAL_FS_URI, conf);
    } else {
      fs = FileSystem.get(conf);
    }
    String filePrefix = conf.get(FILE_PREFIX);
    long sizeThreshold = conf.getLong(SIZE_THRESHOLD, 0);
    long ageThreshold = TimeUnit.MILLISECONDS.convert(conf.getLong(AGE_THRESHOLD_SEC, 0), TimeUnit.MINUTES);
    long startTime = conf.getLong(START_TIME, System.currentTimeMillis());
    Path basePath = new Path(conf.get(OUTPUT_DIR));
    int partition = job.getTaskAttemptID().getTaskID().getId();

    List<FileInfo> incompleteFiles = new ArrayList<>();
    for (FileStatus fileStatus : fs.listStatus(basePath)) {
      if (!fileStatus.isFile()) {
        continue;
      }
      FileInfo fileInfo = FileInfo.fromFileName(fileStatus.getPath().getName());
      if (!fileInfo.prefix.startsWith(filePrefix) ||
        (sizeThreshold != 0 && fileStatus.getLen() >= sizeThreshold) ||
        (ageThreshold != 0 && (startTime - fileInfo.createTime) >= ageThreshold)) {
        continue;
      }
      incompleteFiles.add(fileInfo);
    }

    Collections.sort(incompleteFiles);

    FileInfo fileInfo = partition < incompleteFiles.size() ?
      incompleteFiles.get(partition) : new FileInfo(filePrefix + "-" + NUMBER_FORMAT.format(partition), startTime);

    return new AppendRecordWriter(fs, sizeThreshold, basePath, fileInfo);
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    // no-op
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
    return new OutputCommitter() {
      @Override
      public void setupJob(JobContext jobContext) throws IOException {
        // no-op
      }

      @Override
      public void setupTask(TaskAttemptContext taskContext) throws IOException {
        // no-op
      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
        return false;
      }

      @Override
      public void commitTask(TaskAttemptContext taskContext) throws IOException {
        // no-op
      }

      @Override
      public void abortTask(TaskAttemptContext taskContext) throws IOException {
        // no-op
      }
    };
  }

  /**
   * Information about an output file.
   */
  private static final class FileInfo implements Comparable<FileInfo> {
    private final String prefix;
    private final long createTime;

    public FileInfo(String prefix, long createTime) {
      this.prefix = prefix;
      this.createTime = createTime;
    }

    public String getFileName() {
      return String.format("%s-%d", prefix, createTime);
    }

    public static FileInfo fromFileName(String fileName) {
      int sepIndex = fileName.lastIndexOf('-');
      long createTime = Long.parseLong(fileName.substring(sepIndex + 1));
      String prefix = fileName.substring(0, sepIndex);
      return new FileInfo(prefix, createTime);
    }

    @Override
    public int compareTo(FileInfo o) {
      int comp = prefix.compareTo(o.prefix);
      if (comp != 0) {
        return comp;
      }
      return Long.compare(createTime, o.createTime);
    }

    @Override
    public int hashCode() {
      return Objects.hash(prefix, createTime);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FileInfo that = (FileInfo) o;

      return Objects.equals(prefix, that.prefix) && createTime == that.createTime;
    }
  }

  /**
   * Writes records.
   */
  private static class AppendRecordWriter extends RecordWriter<NullWritable, Text> {
    private static final byte[] LINE_DELIM = "\n".getBytes(StandardCharsets.UTF_8);
    private final FileSystem fs;
    private final Path basePath;
    private final long sizeThreshold;
    private FileInfo fileInfo;
    private DataOutputStream out;
    private long currentSize;

    public AppendRecordWriter(FileSystem fs, long sizeThreshold, Path basePath, FileInfo fileInfo) {
      this.fs = fs;
      this.sizeThreshold = sizeThreshold;
      this.basePath = basePath;
      this.fileInfo = fileInfo;
    }

    @Override
    public void write(NullWritable key, Text value) throws IOException, InterruptedException {
      if (out == null) {
        Path filePath = new Path(basePath, fileInfo.getFileName());
        out = fs.exists(filePath) ? fs.append(filePath) : fs.create(filePath, false);
      }
      if (currentSize > sizeThreshold) {
        rotate();
      }
      byte[] bytes = value.getBytes();
      currentSize += value.getLength();
      out.write(bytes, 0 , value.getLength());
      out.write(LINE_DELIM);
    }

    private void rotate() throws IOException {
      out.close();
      currentSize = 0;

      while (true) {
        fileInfo = new FileInfo(fileInfo.prefix, System.currentTimeMillis());
        Path newPath = new Path(basePath, fileInfo.getFileName());
        if (!fs.exists(newPath)) {
          try {
            out = fs.create(newPath, false);
            break;
          } catch (IOException e) {
            // if it failed to create due to a race condition, just try again with a new timestamp
            if (!fs.exists(newPath)) {
              throw e;
            }
          }
        }
      }
    }

    public synchronized void close(TaskAttemptContext context) throws IOException {
      if (out != null) {
        out.close();
      }
    }
  }
}
