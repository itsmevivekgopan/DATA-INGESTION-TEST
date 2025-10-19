package org.example.transforms;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Objects;

/**
 * A DoFn to parse a CSV line, perform basic cleansing, and convert it to a BigQuery TableRow.
 * It's designed to handle different data types based on a constructor argument.
 */
public class CleanseRecordFn extends DoFn<String, TableRow> {
    private static final Logger LOG = LoggerFactory.getLogger(CleanseRecordFn.class);

    private final String dataType;
    private boolean isHeader = true;

    public CleanseRecordFn(String dataType) {
        this.dataType = Objects.requireNonNull(dataType, "dataType must not be null");
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        String line = c.element();

        // Skip the header row
        if (isHeader) {
            isHeader = false;
            return;
        }

        try {
            String[] parts = line.split(",");
            TableRow row = new TableRow();
            boolean isValid = false;

            if ("customer".equalsIgnoreCase(dataType)) {
                isValid = parseCustomer(parts, row);
            } else if ("transaction".equalsIgnoreCase(dataType)) {
                isValid = parseTransaction(parts, row);
            } else {
                LOG.error("Unsupported data type: package com.example.pipeline.transforms;\n" , dataType);
                // Optionally push to a dead-letter queue here
                return;
            }

            if (isValid) {
                row.set("is_valid", true);
                row.set("ingestion_timestamp", Instant.now().toString());
                c.output(row);
            } else {
                row.set("is_valid", false);
                LOG.warn("Skipping invalid record: {}", line);
                // Optionally push to a dead-letter queue here
            }

        } catch (Exception e) {
            LOG.error("Failed to process line: " + line, e);
        }
    }

    /**
     * Parses a CSV line into a Customer TableRow.
     * customer_id,first_name,last_name,email,registration_date
     */
    private boolean parseCustomer(String[] parts, TableRow row) {
        if (parts.length != 5) return false;

        String customerId = parts[0];
        String registrationDate = parts[4];

        if (customerId == null || customerId.trim().isEmpty() || registrationDate == null || registrationDate.trim().isEmpty()) {
            return false;
        }

        row.set("customer_id", customerId);
        row.set("first_name", parts[1]);
        row.set("last_name", parts[2]);
        row.set("email", parts[3]);

        // Validate date format
        try {
            DateTimeFormatter.ISO_LOCAL_DATE.parse(registrationDate);
            row.set("registration_date", registrationDate);
        } catch (DateTimeParseException e) {
            LOG.warn("Invalid date format for customer {}: {}", customerId, registrationDate);
            return false;
        }

        return true;
    }

    /**
     * Parses a CSV line into a Transaction TableRow.
     * transaction_id,customer_id,amount,transaction_timestamp
     */
    private boolean parseTransaction(String[] parts, TableRow row) {
        if (parts.length != 4) return false;

        String transactionId = parts[0];
        String customerId = parts[1];

        if (transactionId == null || transactionId.trim().isEmpty() || customerId == null || customerId.trim().isEmpty()) {
            return false;
        }

        row.set("transaction_id", transactionId);
        row.set("customer_id", customerId);

        // Validate amount
        try {
            row.set("amount", Double.parseDouble(parts[2]));
        } catch (NumberFormatException e) {
            LOG.warn("Invalid amount format for transaction {}: {}", transactionId, parts[2]);
            return false;
        }

        // Validate timestamp
        try {
            Instant.parse(parts[3]);
            row.set("transaction_timestamp", parts[3]);
        } catch (DateTimeParseException e) {
            LOG.warn("Invalid timestamp format for transaction {}: {}", transactionId, parts[3]);
            return false;
        }

        return true;
    }
}

