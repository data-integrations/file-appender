/*
 * Copyright © 2016-17 Cask Data, Inc.
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

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.api.Engine;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.filesystem.Location;
import org.junit.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for our plugins.
 */
public class PipelineTest extends HydratorTestBase {
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "1.0.0");
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @BeforeClass
  public static void setupTestClass() throws Exception {
    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    // add the data-pipeline artifact and mock plugins
    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    // add our plugins artifact with the data-pipeline artifact as its parent.
    // this will make our plugins available to data-pipeline.
    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
                      parentArtifact,
                      FileAppenderSink.class);
  }

  // ignored until MockSource has some way of controlling the number of splits
  @Ignore
  @Test
  public void testSink() throws Exception {
    Schema inputSchema = Schema.recordOf("test",
                                         Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                         Schema.Field.of("item", Schema.of(Schema.Type.STRING)));
    // create the pipeline config
    String inputName = "sinkTestInput";
    String outputName = "sinkTestOutput";
    String outputDirName = "users";

    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputName));

    Map<String, String> sinkProperties = new HashMap<>();
    sinkProperties.put("name", outputName);
    sinkProperties.put("fileNamePrefix", "users");
    sinkProperties.put(AppendOutputFormat.IS_LOCAL_MODE, "true");
    sinkProperties.put("schema", inputSchema.toString());
    ETLStage sink = new ETLStage("sink", new ETLPlugin(FileAppenderSink.NAME, BatchSink.PLUGIN_TYPE,
                                                       sinkProperties, null));

    ETLBatchConfig pipelineConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .setEngine(Engine.SPARK)
      .build();

    // create the pipeline
    ApplicationId pipelineId = NamespaceId.DEFAULT.app("textSinkTestPipeline");
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, pipelineConfig));

    // write some data to the input fileset
    Map<String, String> users = new HashMap<>();
    users.put("samuel", "wallet");
    users.put("dwayne", "rock");
    users.put("christopher", "cowbell");

    List<StructuredRecord> inputRecords = new ArrayList<>();
    for (Map.Entry<String, String> userEntry : users.entrySet()) {
      String name = userEntry.getKey();
      String item = userEntry.getValue();
      inputRecords.add(StructuredRecord.builder(inputSchema).set("name", name).set("item", item).build());
    }
    DataSetManager<Table> inputManager = getDataset(inputName);
    MockSource.writeInput(inputManager, inputRecords);

    // run the pipeline
    Map<String, String> runtimeArgs = new HashMap<>();
    // the ${dir} macro will be substituted with "users" for our pipeline run
    runtimeArgs.put("dir", outputDirName);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start(runtimeArgs);
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 4, TimeUnit.MINUTES);

    // check the pipeline output
    DataSetManager<FileSet> outputManager = getDataset(outputName);
    FileSet output = outputManager.get();
    Location outputDir = output.getBaseLocation();

    Map<String, Integer> expected = ImmutableMap.of("samuel,wallet", 1, "dwayne,rock", 1, "christopher,cowbell", 1);
    Map<String, Integer> actual = new HashMap<>();
    Set<String> filesFirstRun = new HashSet<>();
    for (Location outputFile : outputDir.list()) {
      if (outputFile.getName().endsWith(".crc") || "_SUCCESS".equals(outputFile.getName())) {
        continue;
      }

      filesFirstRun.add(outputFile.getName());
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(outputFile.getInputStream()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          if (actual.containsKey(line)) {
            actual.put(line, actual.get(line) + 1);
          } else {
            actual.put(line, 1);
          }
        }
      }
    }

    Assert.assertEquals(expected, actual);

    workflowManager.start(runtimeArgs);
    workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 2, 4, TimeUnit.MINUTES);

    Set<String> filesSecondRun = new HashSet<>();
    expected = ImmutableMap.of("samuel,wallet", 2, "dwayne,rock", 2, "christopher,cowbell", 2);
    actual.clear();
    for (Location outputFile : outputDir.list()) {
      if (outputFile.getName().endsWith(".crc") || "_SUCCESS".equals(outputFile.getName())) {
        continue;
      }

      filesSecondRun.add(outputFile.getName());
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(outputFile.getInputStream()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          if (actual.containsKey(line)) {
            actual.put(line, actual.get(line) + 1);
          } else {
            actual.put(line, 1);
          }
        }
      }
    }
    Assert.assertEquals(filesFirstRun, filesSecondRun);
    Assert.assertEquals(expected, actual);
  }

}
