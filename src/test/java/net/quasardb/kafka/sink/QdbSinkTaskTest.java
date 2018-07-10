package net.quasardb.kafka.sink;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.stream.Collectors;

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

import net.quasardb.kafka.common.ConnectorUtils;
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

        this.columns = new Column[NUM_TABLES][];
        this.rows    = new Row[NUM_TABLES][];
        this.tables  = new Table[NUM_TABLES];;

        for (int i = 0; i < NUM_TABLES; ++i) {

            // Generate a column of each value type
            this.columns[i] = Arrays.stream(VALUE_TYPES)
                .map((type) -> {
                        return TestUtils.generateTableColumn(type);
                    })
                .toArray(Column[]::new);

            this.rows[i] = TestUtils.generateTableRows(this.columns[i], NUM_ROWS);
            this.tables[i] = TestUtils.createTable(this.session, this.columns[i]);
        }

        this.task = new QdbSinkTask();
        this.props = new HashMap<>();

        String topicMap = Arrays.stream(this.tables)
            .map((table) -> {
                    // Here we assume kafka topic id == qdb table id
                    return table.getName() + "=" + table.getName();
                })
            .collect(Collectors.joining(","));

        this.props.put(ConnectorUtils.TABLES_CONFIG, topicMap);
    }

    @Test
    public void testPutPrimitives() {
        this.task.start(this.props);
    }
}
