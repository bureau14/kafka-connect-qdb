package net.quasardb.kafka.common.writer;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import net.quasardb.qdb.ts.Writer;
import net.quasardb.kafka.common.TableInfo;

abstract public class RecordWriter {

    abstract public void write(Writer w, TableInfo t, SinkRecord s) throws DataException, RuntimeException;

}
