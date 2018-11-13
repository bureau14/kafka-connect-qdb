package net.quasardb.kafka.common.resolver;

import java.util.Collection;
import java.util.Map;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.errors.DataException;

public class ColumnResolver<T> extends Resolver<T> {

    private static final Logger log = LoggerFactory.getLogger(ColumnResolver.class);
    private String columnName;

    public ColumnResolver(String columnName) {
        this.columnName = columnName;

        log.info("Initializing column table resolver");
    }

    @Override
    public T resolve(SinkRecord record) throws DataException {
        Schema schema = record.valueSchema();
        Object data = record.value();

        // AVRO or JSON with schema
        if (schema != null && data instanceof Struct) {
            return resolve((Struct)data);
        }

        // structured JSON
        if (data instanceof Map) {
            return resolve((Map)data);
        }

        throw new DataException("record is not Avro schema nor structured json, cannot look up column: " + data.toString());
    }

    private T resolve(Struct data) throws DataException {
        Object value = data.get(this.columnName);
        if (value == null) {
            throw new DataException("table column '" + this.columnName + "' not found, cannot resolve: " + data.toString());
        }

        return (T)value;
    }

    private T resolve(Map data) throws DataException {
        Object value = data.get(this.columnName);
        if (value == null) {
            throw new DataException("table column '" + this.columnName + "' not found, cannot resolve: " + data.toString());
        }

        return (T)value;
    }
}
