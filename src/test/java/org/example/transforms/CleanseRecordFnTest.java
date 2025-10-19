package org.example.transforms;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CleanseRecordFnTest {

    @Test
    void testProcessElement_customer_valid() {
        CleanseRecordFn cleanseRecordFn = new CleanseRecordFn("customer");
        DoFn.ProcessContext c = mock(DoFn.ProcessContext.class);
        when(c.element())
                .thenReturn("customer_id,first_name,last_name,email,registration_date") // Header
                .thenReturn("1,John,Doe,john.doe@example.com,2023-01-15"); // Data

        cleanseRecordFn.processElement(c); // Process header
        cleanseRecordFn.processElement(c); // Process data

        ArgumentCaptor<TableRow> argument = ArgumentCaptor.forClass(TableRow.class);
        verify(c, times(1)).output(argument.capture());

        TableRow row = argument.getValue();
        assertEquals("1", row.get("customer_id"));
        assertEquals("John", row.get("first_name"));
        assertEquals("Doe", row.get("last_name"));
        assertEquals("john.doe@example.com", row.get("email"));
        assertEquals("2023-01-15", row.get("registration_date"));
        assertTrue((Boolean) row.get("is_valid"));
        assertNotNull(row.get("ingestion_timestamp"));
    }

    @Test
    void testProcessElement_customer_invalid() {
        CleanseRecordFn cleanseRecordFn = new CleanseRecordFn("customer");
        DoFn.ProcessContext c = mock(DoFn.ProcessContext.class);
        when(c.element())
                .thenReturn("customer_id,first_name,last_name,email,registration_date") // Header
                .thenReturn("1,John,Doe,john.doe@example.com,invalid-date"); // Invalid data

        cleanseRecordFn.processElement(c); // Process header
        cleanseRecordFn.processElement(c); // Process data

        verify(c, never()).output(any(TableRow.class));
    }

    @Test
    void testProcessElement_transaction_valid() {
        CleanseRecordFn cleanseRecordFn = new CleanseRecordFn("transaction");
        DoFn.ProcessContext c = mock(DoFn.ProcessContext.class);
        String timestamp = Instant.now().toString();
        when(c.element())
                .thenReturn("transaction_id,customer_id,amount,transaction_timestamp") // Header
                .thenReturn("txn1,cust1,100.50," + timestamp); // Data

        cleanseRecordFn.processElement(c); // Process header
        cleanseRecordFn.processElement(c); // Process data

        ArgumentCaptor<TableRow> argument = ArgumentCaptor.forClass(TableRow.class);
        verify(c, times(1)).output(argument.capture());

        TableRow row = argument.getValue();
        assertEquals("txn1", row.get("transaction_id"));
        assertEquals("cust1", row.get("customer_id"));
        assertEquals(100.50, row.get("amount"));
        assertEquals(timestamp, row.get("transaction_timestamp"));
        assertTrue((Boolean) row.get("is_valid"));
        assertNotNull(row.get("ingestion_timestamp"));
    }

    @Test
    void testProcessElement_transaction_invalidAmount() {
        CleanseRecordFn cleanseRecordFn = new CleanseRecordFn("transaction");
        DoFn.ProcessContext c = mock(DoFn.ProcessContext.class);
        String timestamp = Instant.now().toString();
        when(c.element())
                .thenReturn("transaction_id,customer_id,amount,transaction_timestamp") // Header
                .thenReturn("txn1,cust1,invalid-amount," + timestamp); // Invalid data

        cleanseRecordFn.processElement(c); // Process header
        cleanseRecordFn.processElement(c); // Process data

        verify(c, never()).output(any(TableRow.class));
    }

    @Test
    void testProcessElement_unsupportedType() {
        CleanseRecordFn cleanseRecordFn = new CleanseRecordFn("unknown");
        DoFn.ProcessContext c = mock(DoFn.ProcessContext.class);
        when(c.element())
                .thenReturn("header,line") // Header
                .thenReturn("some,data"); // Data

        cleanseRecordFn.processElement(c); // Process header
        cleanseRecordFn.processElement(c); // Process data

        verify(c, never()).output(any(TableRow.class));
    }

    @Test
    void testProcessElement_skipsHeader() {
        CleanseRecordFn cleanseRecordFn = new CleanseRecordFn("customer");
        DoFn.ProcessContext c = mock(DoFn.ProcessContext.class);
        when(c.element()).thenReturn("customer_id,first_name,last_name,email,registration_date");

        cleanseRecordFn.processElement(c); // Process header

        verify(c, never()).output(any(TableRow.class));
    }
}
