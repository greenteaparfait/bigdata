/*
 * Copyright (C) 2015 Google Inc.
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

package com.example;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import com.example.common.DataflowExampleUtils;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * An example that reads a file by lines, separates into words, then save them in a bigQuery rable.
 *
 *
 * <p>Basic concepts, also in the MinimalWordCount example:
 * Reading text files; counting a PCollection; writing to bigTable row.
 *
 * <p>New Concepts:
 * <pre>
 *   1. Executing a Pipeline using the Dataflow service
 *   2. Using ParDo with static DoFns defined out-of-line
 *   3. Defining your own pipeline options
 * </pre>
 *
 * <p>Concept #1: you can execute this pipeline using the Dataflow service.
 * These are now command-line options and not hard-coded as they were in the MinimalWordCount
 * example.
 *
 * <p>To execute this pipeline using the Dataflow service, specify pipeline configuration:
 * <pre>{@code
 *   mvn compile exec:java -Dexec.mainClass=com.example.myFileToRow -Dexec.arga="
 *   --project=testcloudstorage-1470232940384
 *   --stagingLocation=gs://floral_ditch_eigenvector/staging
 *   --runner=BlockingDataflowPipelineRunner
 *   --unbounded"
 * }
 */
 
public class myFileToRow {
  private static final Logger LOG = LoggerFactory.getLogger(myFileToRow.class);
  static final int WINDOW_SIZE = 1;  // Default window duration in minutes

  static class AddTimestampFn extends DoFn<String, String> {

    @Override
    public void processElement(ProcessContext c) {
      
      long Timestamp = System.currentTimeMillis();
      c.outputWithTimestamp(c.element(), new Instant(Timestamp));
    }
  }
  
  /**
   * This DoFn tokenizes lines of text into individual words; Then the words become a row in a bigQuery table.
   */
  static class FormatAsTableRowFn extends DoFn<String, TableRow> {
    private final Aggregator<Long, Long> emptyLines =
        createAggregator("emptyLines", new Sum.SumLongFn());

    @Override
    public void processElement(ProcessContext c) {
      if (c.element().trim().isEmpty()) {
        emptyLines.addValue(1L);
      }

      // Split the line into words by comma.
      String[] words = c.element().split(",");

      // Output each word encountered into the output PCollection.
      TableRow row = new TableRow()
          .set("name",words[0])
          .set("rough",words[1])
          .set("hard",words[2])
          .set("slipp",words[3])
          .set("warm",words[4])
          // include a field for the window timestamp
          .set("window_timestamp", c.timestamp().toString());
      c.output(row);
    }
  }

  /**
   * Options supported by {@link Dataflow}.
   *
   * <p>Defining your own configuration options. Here, you can add your own arguments
   * to be processed by the command-line parser, and specify default values for them. You can then
   * access the options values in your pipeline code.
   *
   * <p>Inherits standard configuration options.
   * DataflowExampleUtils inherits DataflowExampleOptions, DataflowPipelineOptions, and ExampleBigQueryTableOptions classes.
   */
  public interface Options extends DataflowExampleUtils.DataflowExampleUtilsOptions, PipelineOptions {
    @Description("Fixed window duration, in minutes")
    @Default.Integer(WINDOW_SIZE)
    Integer getWindowSize();
    void setWindowSize(Integer value);

    @Description("Whether to run the pipeline with unbounded input")
    boolean isUnbounded();
    void setUnbounded(boolean value);

    @Description("Path of the file to read from")
    @Default.String("gs://dataflow-samples/shakespeare/kinglear.txt")
    String getInputFile();
    void setInputFile(String value);

    @Description("Path of the file to write to")
    @Default.InstanceFactory(OutputFactory.class)
    String getOutput();
    void setOutput(String value);

    /**
     * Returns "gs://${YOUR_STAGING_DIRECTORY}/counts.txt" as the default destination.
     */
    class OutputFactory implements DefaultValueFactory<String> {
      @Override
      public String create(PipelineOptions options) {
        DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
        if (dataflowOptions.getStagingLocation() != null) {
          return GcsPath.fromUri(dataflowOptions.getStagingLocation())
              .resolve("counts.txt").toString();
        } else {
          throw new IllegalArgumentException("Must specify --output or --stagingLocation");
        }
      }
    }

  }

  /**
   * Helper method that defines the BigQuery schema used for the output.
  */
  private static TableSchema getSchema() {
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("name").setType("STRING"));
    fields.add(new TableFieldSchema().setName("rough").setType("STRING"));
    fields.add(new TableFieldSchema().setName("hard").setType("STRING"));
    fields.add(new TableFieldSchema().setName("slipp").setType("STRING"));
    fields.add(new TableFieldSchema().setName("warm").setType("STRING"));
    fields.add(new TableFieldSchema().setName("window_timestamp").setType("TIMESTAMP"));
    TableSchema schema = new TableSchema().setFields(fields);
    return schema;
  }

  /**
   * The BigQuery output source is one
   * that supports both bounded and unbounded data. This is a helper method that creates a
   * TableReference from input options, to tell the pipeline where to write its BigQuery results.
  */
  private static TableReference getTableReference(Options options) {
    TableReference tableRef = new TableReference();
    tableRef.setProjectId(options.getProject());
    tableRef.setDatasetId(options.getBigQueryDataset());
    tableRef.setTableId(options.getBigQueryTable());
    return tableRef;
  }

  public static void main(String[] args) throws IOException {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    options.setBigQuerySchema(getSchema());
    DataflowExampleUtils exampleDataflowUtils = new DataflowExampleUtils(options,
      options.isUnbounded());
      
    Pipeline p = Pipeline.create(options);

    /**
     * The Dataflow SDK lets us run the same pipeline with either a bounded or
     * unbounded input source.
     */
    PCollection<String> input;

    if (options.isUnbounded()) {
      LOG.info("Reading from PubSub.");
      /**
       * Read from the PubSub topic. A topic will be created if it wasn't
       * specified as an argument. The data elements' timestamps will come from the pubsub
       * injection.
       */
      input = p.apply(PubsubIO.Read.topic(options.getPubsubTopic())).apply(ParDo.of(new AddTimestampFn()));
    } else {
      /** Else, this is a bounded pipeline. Read from the GCS file. */
      input = p.apply(TextIO.Read.from(options.getInputFile()))
          // Add an element timestamp, using an artificial time just to show windowing.
          // See AddTimestampFn for more detail on this.
          .apply(ParDo.of(new AddTimestampFn()));
    }

    /**
     * Window into fixed windows. The fixed window size for this example defaults to 1
     * minute.
     */
    PCollection<String> windowedInput = input.apply(Window.<String>into(
        FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))));

    // Our pipeline applies the ReadIO transform, and passes the reading to the ParDo transform of
    // FormatAsTableRowFn(). Finally, the table row is written to the bigQuery table.
    //p.apply(TextIO.Read.named("ReadLines").from(options.getInputFile())).apply(ParDo.of(new AddTimestampFn()))
    windowedInput.apply(ParDo.of(new FormatAsTableRowFn()))
     .apply(BigQueryIO.Write.to(getTableReference(options)).withSchema(getSchema()));

    p.run();
  }
}
