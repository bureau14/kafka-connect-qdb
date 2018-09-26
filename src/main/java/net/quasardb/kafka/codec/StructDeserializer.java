package net.quasardb.kafka.codec;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.errors.DataException;

import net.quasardb.qdb.ts.Column;
import net.quasardb.qdb.ts.Value;

/**
 * API for various strategies for converting a Kafka record into
 * a QuasarDB row.
 */
public class StructDeserializer implements Deserializer {

    public void start (Map<String, Object> validatedProps) {
        // Nothing to do here, please move along
    }

    public Object parse(SinkRecord record) {
        if (record.valueSchema() == null ||
            record.valueSchema().type() != Schema.Type.STRUCT) {
            throw new DataException("Expected Struct value, got: " + record.valueSchema());
        }

        return record.value();
    }

    public String tableName(SinkRecord record, Object obj) {
        return record.topic();
    }

    /**
     * Convert a Kafka record to a QuasarDB row.
     * @param columns
     */
    public Value[] convert(Column[] columns, Object obj) {
        Struct record = (Struct)obj;
        Value[] out = new Value[columns.length];

        for (int i = 0; i < columns.length; ++i) {
            Object val = record.get(columns[i].getName());

            if (val == null) {
                out[i] = Value.createNull();
            }

            switch (columns[i].getType()) {
            case DOUBLE:
                out[i] = Value.createDouble((Double)val);
                break;
            case INT64:
                out[i] = Value.createInt64((Long)val);
                break;
            case BLOB:
                out[i] = Value.createSafeBlob((byte[])val);
                break;
            default:
                throw new DataException("Unsupported column type: " + columns[i].toString());
            };
        }

        return out;
    }
}
