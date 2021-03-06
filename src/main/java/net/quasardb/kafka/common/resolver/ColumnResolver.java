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

abstract public class ColumnResolver<T> extends Resolver<T> {

    private static final Logger log = LoggerFactory.getLogger(ColumnResolver.class);
    private String columnName;
    private T suffix;

    public ColumnResolver(String columnName) {
        this.columnName = columnName;
        this.suffix = suffix;

        log.info("Initializing column table resolver");
    }

    public ColumnResolver(String columnName, T suffix) {
        this.columnName = columnName;
        this.suffix = suffix;

        log.info("Initializing column table resolver with suffix: " + suffix);
    }

    @Override
    public T resolve(SinkRecord record) throws DataException {
        Schema schema = record.valueSchema();
        Object data = record.value();

        T result;
        // AVRO or JSON with schema
        if (schema != null && data instanceof Struct) {
            result = resolve((Struct)data);
        } else if (data instanceof Map) {
            result = resolve((Map)data);
        } else {
            throw new DataException("record is not Avro schema nor structured json, cannot look up column: " + data.toString());
        }

        System.err.println("T.resolve, result = " + result.toString());

        if (this.suffix != null) {
            return this.handleSuffix(result, this.suffix);
        } else {
            return result;
        }
    }

    abstract protected T handleSuffix(T result, T suffix);

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
