package org.example.transforms;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CleanseRecordFnTest {

    @Test
    void testProcessElement_customer_valid() throws Exception {
        CleanseRecordFn cleanseRecordFn = new CleanseRecordFn("customer");
        DoFn.ProcessContext c = mock(DoFn.ProcessContext.class);
        when(c.element()).thenReturn("1,John,Doe,john.doe@example.com,2023-01-15");

        cleanseRecordFn.processElement(c); // Header
        cleanseRecordFn.processElement(c); // Data

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
    void testProcessElement_customer_invalid() throws Exception {
        CleanseRecordFn cleanseRecordFn = new CleanseRecordFn("customer");
        DoFn.ProcessContext c = mock(DoFn.ProcessContext.class);
        when(c.element()).thenReturn("1,John,Doe,john.doe@example.com,invalid-date");

        cleanseRecordFn.processElement(c); // Header
        cleanseRecordFn.processElement(c); // Data

        verify(c, never()).output(any(TableRow.class));
    }

    @Test
    void testProcessElement_transaction_valid() throws Exception {
        CleanseRecordFn cleanseRecordFn = new CleanseRecordFn("transaction");
        DoFn.ProcessContext c = mock(DoFn.ProcessContext.class);
        String timestamp = Instant.now().toString();
        when(c.element()).thenReturn("txn1,cust1,100.50," + timestamp);

        cleanseRecordFn.processElement(c); // Header
        cleanseRecordFn.processElement(c); // Data

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
    void testProcessElement_transaction_invalidAmount() throws Exception {
        CleanseRecordFn cleanseRecordFn = new CleanseRecordFn("transaction");
        DoFn.ProcessContext c = mock(DoFn.ProcessContext.class);
        String timestamp = Instant.now().toString();
        when(c.element()).thenReturn("txn1,cust1,invalid-amount," + timestamp);

        cleanseRecordFn.processElement(c); // Header
        cleanseRecordFn.processElement(c); // Data

        verify(c, never()).output(any(TableRow.class));
    }

    @Test
    void testProcessElement_unsupportedType() throws Exception {
        CleanseRecordFn cleanseRecordFn = new CleanseRecordFn("unknown");
        DoFn.ProcessContext c = mock(DoFn.ProcessContext.class);
        when(c.element()).thenReturn("some,data");

        cleanseRecordFn.processElement(c); // Header
        cleanseRecordFn.processElement(c); // Data

        verify(c, never()).output(any(TableRow.class));
    }

    @Test
    void testProcessElement_skipsHeader() throws Exception {
        CleanseRecordFn cleanseRecordFn = new CleanseRecordFn("customer");
        DoFn.ProcessContext c = mock(DoFn.ProcessContext.class);
        when(c.element()).thenReturn("customer_id,first_name,last_name,email,registration_date");

        cleanseRecordFn.processElement(c); // Header

        verify(c, never()).output(any(TableRow.class));
    }
}
