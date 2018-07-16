package net.quasardb.kafka.common;

import org.apache.kafka.connect.sink.SinkRecord;

import net.quasardb.qdb.ts.Column;
import net.quasardb.qdb.ts.Value;

/**
 * API for various strategies for converting a Kafka record into
 * a QuasarDB row.
 */
public interface InputRecordConverter {

    /**
     * Parses Kafka record into internal representation.
     */
    public Object parse(SinkRecord record);

    /**
     * Based on internal representation, resolve the topic name. The function
     * call will be equivalent to `tableName(theRecord, parse(theRecord))`.
     *
     * @param record The kafka record being converted.
     * @param obj Internal representation of object, as returned by #parse.
     */
    public String tableName(SinkRecord record, Object obj);

    /**
     * Convert a Kafka record to a QuasarDB row.
     * @param columns Indexed representation of the columns of the table being
     *                inserted into.
     * @param obj The previously parsed object
     */
    public Value[] convert(Column[] columns, Object obj);

}
