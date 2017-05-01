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
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
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
 * An example that counts words in Shakespeare and includes Dataflow best practices.
 *
 *
 * <p>Basic concepts, also in the MinimalWordCount example:
 * Reading text files; counting a PCollection; writing to bigTable row.
 *
 * <p>New Concepts:
 * <pre>
 *   1. Executing a Pipeline both locally and using the Dataflow service
 *   2. Using ParDo with static DoFns defined out-of-line
 *   3. Building a composite transform
 *   4. Defining your own pipeline options
 * </pre>
 *
 * <p>Concept #1: you can execute this pipeline either locally or using the Dataflow service.
 * These are now command-line options and not hard-coded as they were in the MinimalWordCount
 * example.
 * To execute this pipeline locally, specify general pipeline configuration:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 * }
 * </pre>
 * and a local output file or output prefix on GCS:
 * <pre>{@code
 *   --output=[YOUR_LOCAL_FILE | gs://YOUR_OUTPUT_PREFIX]
 * }</pre>
 *
 * <p>To execute this pipeline using the Dataflow service, specify pipeline configuration:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 *   --stagingLocation=gs://YOUR_STAGING_DIRECTORY
 *   --runner=BlockingDataflowPipelineRunner
 * }
 * </pre>
 * and an output prefix on GCS:
 * <pre>{@code
 *   --output=gs://YOUR_OUTPUT_PREFIX
 * }</pre>
 *
 * <p>The input file defaults to {@code gs://dataflow-samples/shakespeare/kinglear.txt} and can be
 * overridden with {@code --inputFile}.
 */
public class myFileToRow {

  /**
   * A DoFn that sets the data element timestamp. This is a silly method, just for
   * this example, for the bounded data case.
   *
   * <p>Imagine that many ghosts of Shakespeare are all typing madly at the same time to recreate
   * his masterworks. Each line of the corpus will get a random associated timestamp somewhere in a
   * 2-hour period.
   */
  static class AddTimestampFn extends DoFn<String, String> {
    private static final long RAND_RANGE = 7200000; // 2 hours in ms

    @Override
    public void processElement(ProcessContext c) {
      // Generate a timestamp that falls somewhere in the past two hours.
      long randomTimestamp = System.currentTimeMillis()
        - (int) (Math.random() * RAND_RANGE);
      /**
       * Concept #2: Set the data element with that timestamp.
       */
      c.outputWithTimestamp(c.element(), new Instant(randomTimestamp));
    }
  }
  
  /**
   * This DoFn tokenizes lines of text into individual words; Then the words become a row in a bigQuery table.
   * pipeline.
   */
  static class FormatAsTableRowFn extends DoFn<String, TableRow> {
    private final Aggregator<Long, Long> emptyLines =
        createAggregator("emptyLines", new Sum.SumLongFn());

    @Override
    public void processElement(ProcessContext c) {
      if (c.element().trim().isEmpty()) {
        emptyLines.addValue(1L);
      }

      // Split the line into words.
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
   */
  public interface Options extends DataflowExampleUtils.DataflowExampleUtilsOptions, PipelineOptions {
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
   * Concept #6: We'll stream the results to a BigQuery table. The BigQuery output source is one
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

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    options.setBigQuerySchema(getSchema());
    Pipeline p = Pipeline.create(options);

    // Concepts #2 and #3: Our pipeline applies the composite CountWords transform, and passes the
    // static FormatAsTextFn() to the ParDo transform.
    p.apply(TextIO.Read.named("ReadLines").from(options.getInputFile())).apply(ParDo.of(new AddTimestampFn()))
     .apply(ParDo.of(new FormatAsTableRowFn()))
     .apply(BigQueryIO.Write.to(getTableReference(options)).withSchema(getSchema()));
     //.apply(ParDo.of(new FormatAsTextFn()))
     //.apply(TextIO.Write.named("WriteCounts").to(options.getOutput()));

    p.run();
  }
}
