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

package io.cdap.plugin.fileappend;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.lib.FileSetProperties;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
  private final FileAppenderSinkConfig config;
  private Schema outputSchema;

  public FileAppenderSink(FileAppenderSinkConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(failureCollector);
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    config.validateOutputSchema(failureCollector, inputSchema);
    // check collector to make sure both schemas are valid
    failureCollector.getOrThrowException();

    Schema outputSchema = inputSchema;
    Schema configuredSchema = config.getParsedSchema();
    if (configuredSchema != null) {
      outputSchema = configuredSchema;
    }

    String hiveSchema = null;
    if (outputSchema != null) {
      SchemaConverter schemaConverter = new SchemaConverter(true);
      try {
        hiveSchema = schemaConverter.toHiveSchema(outputSchema);
      } catch (UnsupportedTypeException e) {
        failureCollector.addFailure("Could not generate Hive schema: " + e.getMessage(), null)
          .withConfigProperty(FileAppenderSinkConfig.SCHEMA)
          .withStacktrace(e.getStackTrace());
      }
    }

    failureCollector.getOrThrowException();

    // this is only here for unit tests, since ChecksumFileSystem doesn't support append
    String isLocalStr = config.getProperties().getProperties().get(AppendOutputFormat.IS_LOCAL_MODE);
    DatasetProperties properties = FileSetProperties.builder()
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(AppendOutputFormat.class)
      .setBasePath(config.getOutputDir())
      .setEnableExploreOnCreate(true)
      .setExploreFormat("text")
      .setExploreSchema(hiveSchema == null ? "text string" : hiveSchema)
      .setExploreFormatProperty("delimiter", config.getFieldSeparator())
      .setOutputProperty(AppendOutputFormat.FILE_PREFIX, config.getFileNamePrefix())
      .setOutputProperty(AppendOutputFormat.SIZE_THRESHOLD, String.valueOf(config.getSizeThreshold() * 1024 * 1024))
      .setOutputProperty(AppendOutputFormat.AGE_THRESHOLD_SEC, String.valueOf(config.getAgeThreshold()))
      .setOutputProperty(AppendOutputFormat.IS_LOCAL_MODE, isLocalStr)
      .build();
    pipelineConfigurer.createDataset(config.getName(), FileSet.class, properties);
  }

  @Override
  public void prepareRun(final BatchSinkContext context) throws Exception {
    FileSet fileSet = context.getDataset(config.getName());
    Map<String, String> args = new HashMap<>();
    args.put(FileSetProperties.OUTPUT_PROPERTIES_PREFIX + AppendOutputFormat.OUTPUT_DIR,
             fileSet.getBaseLocation().toURI().getPath());
    args.put(FileSetProperties.OUTPUT_PROPERTIES_PREFIX + AppendOutputFormat.START_TIME,
             String.valueOf(context.getLogicalStartTime()));
    context.addOutput(Output.ofDataset(config.getName(), args));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    outputSchema = config.getParsedSchema();
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
      joinedFields.append(config.getFieldSeparator());
      val = input.get(fieldName);
      joinedFields.append(val == null ? "" : val);
    }
    emitter.emit(new KeyValue<>(NullWritable.get(), new Text(joinedFields.toString())));
  }

}
