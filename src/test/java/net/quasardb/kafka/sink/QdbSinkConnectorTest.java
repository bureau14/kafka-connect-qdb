package net.quasardb.kafka.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import net.quasardb.qdb.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QdbSinkConnectorTest {

    private static final int NUM_TASKS = 10;

    private QdbSinkConnector connector;
    private Map<String, String> props;

    @BeforeEach
    void init() {
        connector = new QdbSinkConnector();
        props = new HashMap<>();
    }

    @Test
    public void testTaskConfigs() {
        connector.start(props);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(NUM_TASKS);
        assertEquals(taskConfigs.size(), NUM_TASKS);
        for (int i = 0; i < taskConfigs.size(); ++i) {
            assertEquals(taskConfigs.get(i), props);
        }
    }

    @Test
    public void testTaskClass() {
        assertEquals(QdbSinkTask.class, connector.taskClass());
    }
}
