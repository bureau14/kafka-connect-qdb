package net.quasardb.kafka.common.writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.errors.DataException;

import net.quasardb.qdb.ts.Writer;
import net.quasardb.qdb.ts.Timespec;
import net.quasardb.qdb.ts.Value;
import net.quasardb.qdb.ts.Column;
import net.quasardb.qdb.ts.Table;

import net.quasardb.kafka.common.RecordConverter;

import net.quasardb.kafka.common.writer.RecordWriter;

public class RowRecordWriter extends RecordWriter {

    private static final Logger log = LoggerFactory.getLogger(RowRecordWriter.class);

    public void write(Writer w, Table t, SinkRecord s) throws DataException, RuntimeException {
        Value[] row = RecordConverter.convert (t.getColumns(), s);

        try {
            // Prioritize Kafka's native support for record's timestamps over anything else
            Timespec ts = s.timestamp();

            if (ts == null) {
                // Attempt to read it from the actual record, maybe it is stored as a $timestamp
                ts = RecordConverter.getTimestamp(s);
            }

            if (ts == null) {
                // Default to just the current timestamp
                ts = Timespec.now();
            }

            w.append(t, ts, row);
        } catch (Exception e) {
            log.error("Unable to write record: " + e.getMessage());
            log.error("Record: " + s.toString());
            throw new RuntimeException(e);
        }
    }

}
