package net.quasardb.kafka.sink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import net.quasardb.kafka.common.ConnectorUtils;

public class QdbSinkConnector extends SinkConnector {

    public static final String DEFAULT_CLUSTER_URI = "qdb://127.0.0.1:2836";

    private static final Logger log = LoggerFactory.getLogger(QdbSinkConnector.class);
    private Map<String, String> props;

    @Override
    public void start(Map<String, String> props) {
        this.props = props;

        log.info("Started the QdbSinkConnector.");
    }

    @Override
    public void stop() {
        // TODO Nothing implemented at this point, should probably flush
        log.info("Stopping the QdbSinkConnector.");
    }

    @Override
    public String version() {
        // TODO Currently using Kafka version, in future release use qdb-kafka version
        return AppInfoParser.getVersion();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return QdbSinkTask.class;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
            .define(ConnectorUtils.CLUSTER_URI_CONFIG,
                    Type.STRING,
                    DEFAULT_CLUSTER_URI,
                    Importance.HIGH,
                    "The Cluster uri to connect to.")
            .define(ConnectorUtils.TABLE_FROM_TOPIC_CONFIG,
                    Type.LIST,
                    Importance.HIGH,
                    "Mapping of Kafka topics to QuasarDB timeseries.")
            ;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>(props);
            configs.add(config);
        }
        return configs;
    }

}
