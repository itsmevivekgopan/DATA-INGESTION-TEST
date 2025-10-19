package org.example;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.example.transforms.CleanseRecordFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Dataflow pipeline to ingest CSV data from GCS into BigQuery.
 * This pipeline is templated to handle both Customer and Transaction data.
 */
public class DataIngestionPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(DataIngestionPipeline.class);

    // Define pipeline options for runtime configuration
    public interface IngestionOptions extends PipelineOptions {
        @Description("GCS path of the input CSV file")
        @Required
        String getInputFile();
        void setInputFile(String value);

        @Description("BigQuery output table spec, e.g., project:dataset.table")
        @Required
        String getOutputTable();
        void setOutputTable(String value);

        @Description("Data type being ingested (e.g., 'customer' or 'transaction')")
        @Required
        String getDataType();
        void setDataType(String value);
    }

    public static void main(String[] args) {
        IngestionOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(IngestionOptions.class);
        run(options);
    }

    public static void run(IngestionOptions options) {
        Pipeline p = Pipeline.create(options);

        // Step 1: Read lines from the input CSV file in GCS.
        PCollection<String> lines = p
                .apply("ReadFromGCS", TextIO.read().from(options.getInputFile()))
                .setName("Read CSV lines");

        // Step 2: Cleanse records and convert to TableRow objects.
        // The CleanseRecordFn handles parsing, validation, and conversion.
        PCollection<TableRow> tableRows = lines.apply("CleanseAndConvertToTableRow",
                        ParDo.of(new CleanseRecordFn(options.getDataType())))
                .setName("Cleanse and Convert");

        // Step 2.5: Add validation flag to each record
        PCollection<TableRow> tableRowsWithValidation = tableRows.apply("AddValidationFlag",
                        ParDo.of(new DoFn<TableRow, TableRow>() {
                            @ProcessElement
                            public void processElement(@Element TableRow row, OutputReceiver<TableRow> out) {
                                // Add is_valid field - default to true if records pass CleanseRecordFn

                                // Need to add validation rules for fields.
                                row.set("is_valid", true);
                                out.output(row);
                            }
                        }))
                .setName("Add Validation Field");

        // Step 3: Write the TableRow objects to the specified BigQuery table.
        tableRowsWithValidation.apply("WriteToBigQuery", BigQueryIO.writeTableRows()
                        .to(options.getOutputTable())
                        .withCreateDisposition(CreateDisposition.CREATE_NEVER) // Table must already exist
                        .withWriteDisposition(WriteDisposition.WRITE_APPEND) // Append to existing table
                        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS));

        // Run the pipeline.
        p.run().waitUntilFinish();
    }
}
