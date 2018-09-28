package net.quasardb.kafka.common;

import java.util.Collection;
import java.util.Map;
import java.util.HashMap;

import org.apache.kafka.connect.errors.DataException;

import net.quasardb.kafka.codec.Deserializer;

public class ConnectorUtils {

    public static final String CLUSTER_URI_CONFIG = "qdb.cluster";
    public static final String TABLE_FROM_TOPIC_CONFIG = "qdb.table_from_topic";

    /**
     * Defines the class to be used to read/write records to/from Kafka topics.
     */
    public static final String DESERIALIZER_CONFIG = "qdb.codec.deserializer";


    /**
     * Defines optional transformation step for JSON input.
     */
    public static final String JSON_TRANSFORM_CONFIG = "qdb.codec.json.transform";

    /**
     * Parses table input config and emits a mapping of Kafka topics to
     * QuasarDB tables.
     */
    public static Map<String, String> parseTableFromTopic(Collection<String> xs) {
        Map<String, String> out = new HashMap();

        for (String x : xs) {
            String[] tokens = x.split("=");

            if (tokens.length != 2) {
                throw new DataException("Incorrectly formatted table config: expected 'topic=table', got: " + x);
            }

            out.put(tokens[0], tokens[1]);
        }

        return out;
    }

    /**
     * Parses codec deserializer String and loads class by this name.
     */
    public static Deserializer createDeserializer(Map<String, Object> validatedProps) {
        Class in = (Class)validatedProps.get(ConnectorUtils.DESERIALIZER_CONFIG);
        try {
            Deserializer out = Deserializer.class.cast(in.newInstance());
            out.start(validatedProps);
            return out;
        } catch (InstantiationException e) {
            throw new DataException(e);
        } catch (IllegalAccessException e) {
            throw new DataException(e);
        }
    }
}
