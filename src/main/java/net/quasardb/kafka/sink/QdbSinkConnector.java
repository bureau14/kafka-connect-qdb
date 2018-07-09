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


public class QdbSinkConnector extends SinkConnector {

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
        return new ConfigDef();
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
