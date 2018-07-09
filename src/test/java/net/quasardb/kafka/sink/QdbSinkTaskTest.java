package net.quasardb.kafka.sink;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class QdbSinkTaskTest {

    private static final int NUM_TASKS = 10;

    private QdbSinkTask task;
    private Map<String, String> props;

    @Before
    public void setup() {
        task = new QdbSinkTask();
        props = new HashMap<>();
    }

    @Test
    public void testPutPrimitives() {
        task.start(props);
    }
}
