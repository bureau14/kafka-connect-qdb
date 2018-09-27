package net.quasardb.kafka.codec;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.errors.DataException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


import net.quasardb.qdb.ts.Column;
import net.quasardb.qdb.ts.Value;

/**
 * API for various strategies for converting a Kafka record into
 * a QuasarDB row.
 */
public class JsonDeserializer implements Deserializer {

    private ObjectMapper jsonMapper;

    public void start(Map<String, Object> validatedProps) {
        this.jsonMapper = new ObjectMapper();
    }

    public Object parse(SinkRecord record) {
        if (record.valueSchema() == null ||
            record.valueSchema().type() != Schema.Type.STRING) {
            throw new DataException("Expected String value, got: " + record.valueSchema());
        }

        try {
            return this.jsonMapper.readTree((String)record.value());
        } catch (IOException e) {
            throw new DataException(e);
        }
    }

    public String tableName(SinkRecord record, Object obj) {
        return record.topic();
    }

    /**
     * Convert a plain JSON String record to a QuasarDB row.
     */
    public Value[] convert(Column[] columns, Object obj) {
        JsonNode tree = (JsonNode)obj;

        Value[] out = new Value[columns.length];

        for (int i = 0; i < columns.length; ++i) {
            Column c = columns[i];

            JsonNode n = tree.get(c.getName());

            switch (columns[i].getType()) {
            case INT64:
                out[i] = Value.createInt64(n.asLong());
                break;
            case DOUBLE:
                out[i] = Value.createDouble(n.asDouble());
                break;
            case BLOB:
                try {
                    out[i] = Value.createSafeBlob(n.binaryValue());
                } catch (IOException e) {
                    throw new DataException(e);
                }
                break;
            }
        }

        return out;
    }
}
