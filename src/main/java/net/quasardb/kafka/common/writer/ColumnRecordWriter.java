package net.quasardb.kafka.common.writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.errors.DataException;

import net.quasardb.qdb.ts.Writer;
import net.quasardb.qdb.ts.Timespec;
import net.quasardb.qdb.ts.Column;
import net.quasardb.qdb.ts.Value;

import net.quasardb.kafka.common.TableInfo;
import net.quasardb.kafka.common.RecordConverter;

import net.quasardb.kafka.common.resolver.Resolver;
import net.quasardb.kafka.common.writer.RecordWriter;

public class ColumnRecordWriter extends RecordWriter {

    private static final Logger log = LoggerFactory.getLogger(ColumnRecordWriter.class);

    private Resolver<String> columnResolver;
    private Resolver<String> valueResolver;

    public ColumnRecordWriter(Resolver<String> columnResolver, Resolver<String> valueResolver) {
        this.columnResolver = columnResolver;
        this.valueResolver = valueResolver;
    }

    public void write(Writer w, TableInfo t, SinkRecord s) throws DataException, RuntimeException {
        String columnName = this.columnResolver.resolve(s);
        String valueName = this.valueResolver.resolve(s);

        int columnIndex = t.getTable().columnIndexById(columnName);
        Column c = t.getTable().getColumns()[columnIndex];

        Value val = RecordConverter.convert(c, valueName, s);

        log.debug("has value: " + val.toString());

        Value[] row = { val };

        int offset = t.getOffset() + columnIndex;

        try {
            Timespec ts = (s.timestamp() == null
                           ? Timespec.now()
                           : new Timespec(s.timestamp()));

            log.debug("has timespec: " + ts.toString());

            w.append(offset, ts, row);

        } catch (Exception e) {
            log.error("Unable to write record: " + e.getMessage());
            log.error("Record: " + s.toString());
            throw new RuntimeException(e);
        }
    }
}
