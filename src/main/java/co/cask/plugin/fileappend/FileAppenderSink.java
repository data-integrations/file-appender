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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Writes to files in a directory. Will append to existing files until they are larger or older than configurable
 * thresholds.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(FileAppenderSink.NAME)
@Description("Writes to files in a directory. Will append to existing files until they are larger or older " +
  "than configurable thresholds.")
public class FileAppenderSink extends BatchSink<StructuredRecord, NullWritable, Text> {
  public static final String NAME = "FileAppender";
  private final Conf config;
  private Schema outputSchema;

  /**
   * Config properties for the plugin.
   */
  public static class Conf extends PluginConfig {

    @Description("The name of the FileSet to write to.")
    private String name;

    @Nullable
    @Description("Optional directory within the FileSet to write to.")
    private String outputDir;

    @Description("The prefix to use for all files in the FileSet.")
    private String fileNamePrefix;

    @Nullable
    @Description("Rotate files after they are this large (in mb). 0 means no limit. Defaults to 100.")
    private Long sizeThreshold;

    @Nullable
    @Description("Rotate files after they are this old (in minutes). 0 means no limit. Defaults to 60.")
    private Long ageThreshold;

    @Nullable
    @Description("The separator to use to join input record fields together. Defaults to ','.")
    private String fieldSeparator;

    @Nullable
    @Description("The schema of the FileSet.")
    private String schema;

    public Conf() {
      fieldSeparator = ",";
      sizeThreshold = 100L;
      ageThreshold = 60L;
    }

    public void validate() {
      if (sizeThreshold < 0) {
        throw new IllegalArgumentException("sizeThreshold must be at least 0.");
      }
      if (ageThreshold < 0) {
        throw new IllegalArgumentException("ageThreshold must be at least 0.");
      }
    }
  }

  public FileAppenderSink(Conf config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate();
    // TODO: validate input schema
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    Schema outputSchema = inputSchema;
    if (config.schema != null) {
      try {
        outputSchema = Schema.parseJson(config.schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Could not parse schema: " + e.getMessage(), e);
      }
    }

    String hiveSchema = null;
    if (outputSchema != null) {

      if (inputSchema != null) {
        for (Schema.Field outputField : outputSchema.getFields()) {
          Schema.Field inField = inputSchema.getField(outputField.getName());
          if (inField == null) {
            throw new IllegalArgumentException(
              String.format("Field '%s' is not in the input schema.", outputField.getName()));
          } else if (!inField.getSchema().equals(outputField.getSchema())) {
            throw new IllegalArgumentException(
              String.format("Schema for field '%s' does not match the schema of the input field (%s)",
                            outputField.getName(), inField.getSchema()));
          }
        }
      }

      SchemaConverter schemaConverter = new SchemaConverter(true);
      try {
        hiveSchema = schemaConverter.toHiveSchema(outputSchema);
      } catch (UnsupportedTypeException e) {
        throw new IllegalArgumentException("Could not generate Hive schema: " + e.getMessage(), e);
      }
    }

    // this is only here for unit tests, since ChecksumFileSystem doesn't support append
    String isLocalStr = config.getProperties().getProperties().get(AppendOutputFormat.IS_LOCAL_MODE);
    DatasetProperties properties = FileSetProperties.builder()
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(AppendOutputFormat.class)
      .setBasePath(config.outputDir)
      .setEnableExploreOnCreate(true)
      .setExploreFormat("text")
      .setExploreSchema(hiveSchema == null ? "text string" : hiveSchema)
      .setExploreFormatProperty("delimiter", config.fieldSeparator)
      .setOutputProperty(AppendOutputFormat.FILE_PREFIX, config.fileNamePrefix)
      .setOutputProperty(AppendOutputFormat.SIZE_THRESHOLD, String.valueOf(config.sizeThreshold * 1024 * 1024))
      .setOutputProperty(AppendOutputFormat.AGE_THRESHOLD_SEC, String.valueOf(config.ageThreshold))
      .setOutputProperty(AppendOutputFormat.IS_LOCAL_MODE, isLocalStr)
      .build();
    pipelineConfigurer.createDataset(config.name, FileSet.class, properties);
  }

  @Override
  public void prepareRun(final BatchSinkContext context) throws Exception {
    FileSet fileSet = context.getDataset(config.name);
    Map<String, String> args = new HashMap<>();
    args.put(FileSetProperties.OUTPUT_PROPERTIES_PREFIX + AppendOutputFormat.OUTPUT_DIR,
             fileSet.getBaseLocation().toURI().getPath());
    args.put(FileSetProperties.OUTPUT_PROPERTIES_PREFIX + AppendOutputFormat.START_TIME,
             String.valueOf(context.getLogicalStartTime()));
    context.addOutput(Output.ofDataset(config.name, args));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    outputSchema = config.schema == null ? null : Schema.parseJson(config.schema);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Text>> emitter) throws Exception {
    StringBuilder joinedFields = new StringBuilder();
    Iterator<Schema.Field> fieldIter = input.getSchema().getFields().iterator();
    if (!fieldIter.hasNext()) {
      // shouldn't happen
      return;
    }

    Object val = input.get(fieldIter.next().getName());
    if (val != null) {
      joinedFields.append(val);
    }
    while (fieldIter.hasNext()) {
      String fieldName = fieldIter.next().getName();
      if (outputSchema != null && outputSchema.getField(fieldName) == null) {
        continue;
      }
      joinedFields.append(config.fieldSeparator);
      val = input.get(fieldName);
      joinedFields.append(val == null ? "" : val);
    }
    emitter.emit(new KeyValue<>(NullWritable.get(), new Text(joinedFields.toString())));
  }

}
