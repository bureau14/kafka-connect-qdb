package net.quasardb.kafka.common;

import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import org.apache.kafka.connect.errors.DataException;

public class ConnectorUtils {

    public static final String CLUSTER_URI_CONFIG = "qdb.cluster";
    public static final String TABLE_FROM_TOPIC_CONFIG = "qdb.table_from_topic";

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
}
