package net.quasardb.kafka.common;

import java.util.Collection;
import java.util.Map;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.errors.DataException;

public class ColumnTableResolver extends TableResolver {

    private static final Logger log = LoggerFactory.getLogger(ColumnTableResolver.class);
    private String columnName;

    public ColumnTableResolver(String columnName) {
        this.columnName = columnName;

        log.info("Initializing column table resolver");
    }

    @Override
    public String resolve(SinkRecord record) throws DataException {
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

        return record.topic();
    }

    private String resolve(Struct data) throws DataException {
        Object value = data.get(this.columnName);
        if (value == null) {
            throw new DataException("table column '" + this.columnName + "' not found, cannot resolve: " + data.toString());
        }

        if (value instanceof String) {
            return (String)value;
        } else {
            throw new DataException("table column '" + this.columnName + "' found, but not a string: " + data.toString());
        }
    }

    private String resolve(Map data) throws DataException {
        Object value = data.get(this.columnName);
        if (value == null) {
            throw new DataException("table column '" + this.columnName + "' not found, cannot resolve: " + data.toString());
        }

        if (value instanceof String) {
            return (String)value;
        } else {
            throw new DataException("table column '" + this.columnName + "' found, but not a string: " + data.toString());
        }
    }

}
