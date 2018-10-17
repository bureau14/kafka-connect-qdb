package net.quasardb.kafka.common;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.errors.DataException;

import net.quasardb.qdb.ts.Column;
import net.quasardb.qdb.ts.Value;

/**
 * API for various strategies for converting a Kafka record into
 * a QuasarDB row.
 */
public class RecordConverter {


    private static final Logger log = LoggerFactory.getLogger(RecordConverter.class);

    public static Value[] convert(Column[] columns, SinkRecord record) throws DataException {
        return convert(columns, record.valueSchema(), record.value());
    }

    private static Value[] convert(Column[] columns, Schema schema, Object data) throws DataException {

        // AVRO or JSON with schema
        if (schema != null && data instanceof Struct) {
            return convert(columns, (Struct)data);
        }

        // structured JSON
        if (data instanceof Map) {
            return convert(columns, (Map)data);
        }

        throw new DataException("Only schemaful converters are currently supported, input: " + data.toString());
    }

    private static Value[] convert(Column[] columns, Struct data) throws DataException {
        Value[] out = new Value[columns.length];

        for (int i = 0; i < columns.length; ++i) {
            Column c = columns[i];

            Object value = data.get(c.getName());

            if (value != null) {
                switch(c.getType()) {
                case INT64:
                    if (value instanceof Long) {
                        out[i] = Value.createInt64((Long)value);
                    } else {
                        log.warn("Ignoring int64 column '" + c.getName () + "': expected Long value, got: " + value.getClass());
                        out[i] = Value.createNull();
                    }
                    break;
                case DOUBLE:
                    if (value instanceof Double) {
                        out[i] = Value.createDouble((Double)value);
                    } else {
                        log.warn("Ignoring double column '" + c.getName () + "': expected Double value, got: " + value.getClass());
                        out[i] = Value.createNull();
                    }
                    break;
                case BLOB:
                    if (value instanceof byte[]) {
                        out[i] = Value.createSafeBlob((byte[])value);
                    } else {
                        log.warn("Ignoring blob column '" + c.getName () + "': expected String value, got: " + value.getClass());
                        out[i] = Value.createNull();
                    }
                    break;

                }

            } else {
                log.warn("key not found, setting null value: " + c.getName());
                out[i]  = Value.createNull();
            }
        }

        return out;
    }

    private static Value[] convert(Column[] columns, Map data) throws DataException {
        Value[] out = new Value[columns.length];

        for (int i = 0; i < columns.length; ++i) {
            Column c = columns[i];

            Object value = data.get(c.getName());

            if (value != null) {
                switch(columns[i].getType()) {
                case INT64:
                    if (value instanceof Long) {
                        out[i] = Value.createInt64((Long)value);
                    } else {
                        log.warn("Ignoring int64 column '" + c.getName () + "': expected Long value, got: " + value.getClass());
                        out[i] = Value.createNull();
                    }
                    break;
                case DOUBLE:
                    if (value instanceof Double) {
                        out[i] = Value.createDouble((Double)value);
                    } else {
                        log.warn("Ignoring double column '" + c.getName () + "': expected Double value, got: " + value.getClass());
                        out[i] = Value.createNull();
                    }
                    break;
                case BLOB:
                    if (value instanceof String) {
                        out[i] = Value.createSafeString((String)value);
                    } else {
                        log.warn("Ignoring blob column '" + c.getName () + "': expected String value, got: " + value.getClass());
                        out[i] = Value.createNull();
                    }
                    break;
                }

            } else {
                log.warn("key not found, setting null value: " + c.getName());
                out[i]  = Value.createNull();
            }
        }

        return out;
    }

}
