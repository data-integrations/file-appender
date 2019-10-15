package io.cdap.plugin.fileappend;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Config properties for the plugin.
 */
public class FileAppenderSinkConfig extends PluginConfig {
  public static final String SCHEMA = "schema";
  public static final String SIZE_THRESHOLD = "sizeThreshold";
  public static final String AGE_THRESHOLD = "ageThreshold";

  @Description("The name of the FileSet to write to.")
  private final String name;

  @Nullable
  @Description("Optional directory within the FileSet to write to.")
  private final String outputDir;

  @Description("The prefix to use for all files in the FileSet.")
  private final String fileNamePrefix;

  @Nullable
  @Description("Rotate files after they are this large (in mb). 0 means no limit. Defaults to 100.")
  private final Long sizeThreshold;

  @Nullable
  @Description("Rotate files after they are this old (in minutes). 0 means no limit. Defaults to 60.")
  private final Long ageThreshold;

  @Nullable
  @Description("The separator to use to join input record fields together. Defaults to ','.")
  private final String fieldSeparator;

  @Nullable
  @Description("The schema of the FileSet.")
  private final String schema;

  public FileAppenderSinkConfig(
    String name,
    @Nullable String outputDir,
    String fileNamePrefix,
    @Nullable Long sizeThreshold,
    @Nullable Long ageThreshold,
    @Nullable String fieldSeparator,
    @Nullable String schema
  ) {
    this.name = name;
    this.outputDir = outputDir;
    this.fileNamePrefix = fileNamePrefix;
    this.sizeThreshold = sizeThreshold;
    this.ageThreshold = ageThreshold;
    this.fieldSeparator = fieldSeparator;
    this.schema = schema;
  }

  private FileAppenderSinkConfig(Builder builder) {
    name = builder.name;
    outputDir = builder.outputDir;
    fileNamePrefix = builder.fileNamePrefix;
    sizeThreshold = builder.sizeThreshold;
    ageThreshold = builder.ageThreshold;
    fieldSeparator = builder.fieldSeparator;
    schema = builder.schema;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(FileAppenderSinkConfig copy) {
    return new Builder()
      .setName(copy.getName())
      .setOutputDir(copy.getOutputDir())
      .setFileNamePrefix(copy.getFileNamePrefix())
      .setSizeThreshold(copy.getSizeThreshold())
      .setAgeThreshold(copy.getAgeThreshold())
      .setFieldSeparator(copy.getFieldSeparator())
      .setSchema(copy.getSchema());
  }

  public void validate(FailureCollector failureCollector) {
    if (sizeThreshold < 0) {
      failureCollector.addFailure("Size Threshold must be at least 0.", null)
        .withConfigProperty(SIZE_THRESHOLD);
    }
    if (ageThreshold < 0) {
      failureCollector.addFailure("Age Threshold must be at least 0.", null)
        .withConfigProperty(AGE_THRESHOLD);
    }
  }

  public void validateOutputSchema(FailureCollector failureCollector, @Nullable Schema inputSchema) {
    Schema parsedSchema = null;
    try {
      parsedSchema = getParsedSchema();
    } catch (IllegalArgumentException e) {
      failureCollector.addFailure(e.getMessage(), null)
        .withConfigProperty(SCHEMA)
        .withStacktrace(e.getStackTrace());
    }
    if (inputSchema != null && parsedSchema != null) {
      for (Schema.Field outputField : parsedSchema.getFields()) {
        Schema.Field inField = inputSchema.getField(outputField.getName());
        if (inField == null) {
          failureCollector.addFailure(String.format("Field '%s' must be present in the input schema.",
                                                    outputField.getName()), null)
            .withOutputSchemaField(outputField.getName());
        } else {
          Schema inFieldSchema = getNonNullableSchema(inField.getSchema());
          Schema outFieldSchema = getNonNullableSchema(outputField.getSchema());
          if (!inFieldSchema.equals(outFieldSchema)) {
            failureCollector.addFailure(
              String.format("Field '%s' has type '%s' that does not match with input field schema '%s'.",
                            outputField.getName(), outFieldSchema.getDisplayName(), inFieldSchema.getDisplayName()),
              String.format("Ensure output schema of '%s' matches input schema.", outputField.getName()))
              .withInputSchemaField(outputField.getName())
              .withOutputSchemaField(outputField.getName());
          }
        }
      }
    }
  }

  public String getName() {
    return name;
  }

  @Nullable
  public String getOutputDir() {
    return outputDir;
  }

  public String getFileNamePrefix() {
    return fileNamePrefix;
  }

  @Nullable
  public Long getSizeThreshold() {
    return sizeThreshold;
  }

  @Nullable
  public Long getAgeThreshold() {
    return ageThreshold;
  }

  @Nullable
  public String getFieldSeparator() {
    return fieldSeparator;
  }

  @Nullable
  public String getSchema() {
    return schema;
  }

  @Nullable
  public Schema getParsedSchema() {
    try {
      return schema != null ? Schema.parseJson(schema) : null;
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid schema :" + e.getMessage(), e);
    }
  }

  private static Schema getNonNullableSchema(Schema schema) {
    return schema.isNullable() ? schema.getNonNullable() : schema;
  }

  public static final class Builder {
    private String name;
    private String outputDir;
    private String fileNamePrefix;
    private Long sizeThreshold;
    private Long ageThreshold;
    private String fieldSeparator;
    private String schema;

    private Builder() {
    }

    public Builder setName(String val) {
      name = val;
      return this;
    }

    public Builder setOutputDir(@Nullable String val) {
      outputDir = val;
      return this;
    }

    public Builder setFileNamePrefix(String val) {
      fileNamePrefix = val;
      return this;
    }

    public Builder setSizeThreshold(@Nullable Long val) {
      sizeThreshold = val;
      return this;
    }

    public Builder setAgeThreshold(@Nullable Long val) {
      ageThreshold = val;
      return this;
    }

    public Builder setFieldSeparator(@Nullable String val) {
      fieldSeparator = val;
      return this;
    }

    public Builder setSchema(@Nullable String val) {
      schema = val;
      return this;
    }

    public FileAppenderSinkConfig build() {
      return new FileAppenderSinkConfig(this);
    }
  }
}
