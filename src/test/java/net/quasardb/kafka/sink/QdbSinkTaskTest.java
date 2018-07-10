package net.quasardb.kafka.sink;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import net.quasardb.qdb.Session;

import net.quasardb.qdb.ts.Column;
import net.quasardb.qdb.ts.Row;
import net.quasardb.qdb.ts.Value;
import net.quasardb.qdb.ts.Timespec;
import net.quasardb.qdb.ts.Table;
import net.quasardb.qdb.ts.Tables;

import net.quasardb.kafka.common.TestUtils;

public class QdbSinkTaskTest {

    private static final int    NUM_TASKS   = 10;
    private static final int    NUM_TABLES  = 4;
    private static final int    NUM_ROWS    = 1000;
    private static Value.Type[] VALUE_TYPES = { Value.Type.INT64,
                                                Value.Type.DOUBLE,
                                                Value.Type.TIMESTAMP,
                                                Value.Type.BLOB };

    private Session             session;
    private QdbSinkTask         task;
    private Map<String, String> props;
    private Column[][]          columns;
    private Row[][]             rows;
    private Table[]             tables;

    @Before
    public void setup() throws IOException {
        this.session = TestUtils.createSession();

        for (int i = 0; i < NUM_TABLES; ++i) {

            // Generate a column of each value type
            Column[] cols = Arrays.stream(VALUE_TYPES)
                .map((type) -> {
                        return TestUtils.generateTableColumn(type);
                    })
                .toArray(Column[]::new);

            Row[] rows = TestUtils.generateTableRows(cols, NUM_ROWS);
            Table table = TestUtils.createTable(this.session, cols);

            System.out.println("table: " + table.toString());

        }

        task = new QdbSinkTask();
        props = new HashMap<>();
    }

    @Test
    public void testPutPrimitives() {
        task.start(props);
    }
}
