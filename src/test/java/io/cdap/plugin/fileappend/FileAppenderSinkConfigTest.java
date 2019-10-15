package io.cdap.plugin.fileappend;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class FileAppenderSinkConfigTest {
  private static final String MOCK_STAGE = "mockStage";
  private static final Schema VALID__SCHEMA = Schema.recordOf(
    "schema", Schema.Field.of("ID", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
  private static final FileAppenderSinkConfig VALID_CONFIG = new FileAppenderSinkConfig(
    "",
    null,
    "",
    0L,
    0L,
    null,
    VALID__SCHEMA.toString()
  );

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testInvalidSizeThreshold() {
    FileAppenderSinkConfig config = FileAppenderSinkConfig.builder(VALID_CONFIG)
      .setSizeThreshold(-1L)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, FileAppenderSinkConfig.SIZE_THRESHOLD);
  }

  @Test
  public void testInvalidAgeThreshold() {
    FileAppenderSinkConfig config = FileAppenderSinkConfig.builder(VALID_CONFIG)
      .setAgeThreshold(-1L)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, FileAppenderSinkConfig.AGE_THRESHOLD);
  }

  @Test
  public void testValidOutputSchema() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validateOutputSchema(failureCollector, VALID__SCHEMA);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testInvalidSchemaFormat() {
    FileAppenderSinkConfig config = FileAppenderSinkConfig.builder(VALID_CONFIG)
      .setSchema("{[}")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validateOutputSchema(failureCollector, null);
    assertValidationFailed(failureCollector, FileAppenderSinkConfig.SCHEMA);
    assertValidationFailedWithStacktrace(failureCollector);
  }

  @Test
  public void testOutputFieldIsMissingInInputSchemaValidation() {
    Schema inputSchema = Schema.recordOf(
      "schema", Schema.Field.of("UID", Schema.nullableOf(Schema.of(Schema.Type.LONG))));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validateOutputSchema(failureCollector, inputSchema);
    assertOutputSchemaValidationFailed(failureCollector, "ID");
  }

  @Test
  public void testOutputFieldHasIncorrectSchemaValidation() {
    Schema inputSchema = Schema.recordOf(
      "schema", Schema.Field.of("ID", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validateOutputSchema(failureCollector, inputSchema);
    assertOutputSchemaValidationFailed(failureCollector, "ID");
    assertInputSchemaValidationFailed(failureCollector, "ID");
  }

  private static void assertValidationFailed(MockFailureCollector failureCollector, String paramName) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(1, failureList.size());
    ValidationFailure failure = failureList.get(0);
    List<ValidationFailure.Cause> causeList = getCauses(failure, CauseAttributes.STAGE_CONFIG);
    Assert.assertEquals(1, causeList.size());
    ValidationFailure.Cause cause = causeList.get(0);
    Assert.assertEquals(paramName, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  private static void assertInputSchemaValidationFailed(MockFailureCollector failureCollector, String fieldName) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(1, failureList.size());
    ValidationFailure failure = failureList.get(0);
    List<ValidationFailure.Cause> causeList = getCauses(failure, CauseAttributes.INPUT_SCHEMA_FIELD);
    Assert.assertEquals(1, causeList.size());
    ValidationFailure.Cause cause = causeList.get(0);
    Assert.assertEquals(fieldName, cause.getAttribute(CauseAttributes.INPUT_SCHEMA_FIELD));
  }

  private static void assertOutputSchemaValidationFailed(MockFailureCollector failureCollector, String fieldName) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(1, failureList.size());
    ValidationFailure failure = failureList.get(0);
    List<ValidationFailure.Cause> causeList = getCauses(failure, CauseAttributes.OUTPUT_SCHEMA_FIELD);
    Assert.assertEquals(1, causeList.size());
    ValidationFailure.Cause cause = causeList.get(0);
    Assert.assertEquals(fieldName, cause.getAttribute(CauseAttributes.OUTPUT_SCHEMA_FIELD));
  }

  private static void assertValidationFailedWithStacktrace(MockFailureCollector failureCollector) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(1, failureList.size());
    ValidationFailure failure = failureList.get(0);
    List<ValidationFailure.Cause> causeList = getCauses(failure, CauseAttributes.STACKTRACE);
    Assert.assertEquals(1, causeList.size());
  }

  @Nonnull
  private static List<ValidationFailure.Cause> getCauses(ValidationFailure failure, String stacktrace) {
    return failure.getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(stacktrace) != null)
      .collect(Collectors.toList());
  }
}
