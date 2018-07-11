package net.quasardb.kafka.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import java.io.IOException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import org.apache.kafka.connect.errors.DataException;

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

    private static SinkRecord[] rowToRecords(String topic,
                                             Integer partition,
                                             Column[] columns,
                                             Row row) {
        return rowToRecords(topic, partition, columns, row.getValues());
    }

    private static SinkRecord[] rowToRecords(String topic,
                                             Integer partition,
                                             Column[] columns,
                                             Value[] row) {
        assertEquals(row.length, columns.length);

        SinkRecord[] records = new SinkRecord[columns.length];
        for (int i = 0; i < columns.length; ++i) {
            records[i] = columnToRecord(topic, partition, columns[i], row[i]);
        }

        return records;
    }

    private static SinkRecord columnToRecord(String topic,
                                             Integer partition,
                                             Column column,
                                             Value v) {
        switch (v.getType()) {
        case INT64:
            break;
        case DOUBLE:
            break;
        case TIMESTAMP:
            break;
        case BLOB:
            break;
        }

        return null;
    }

    @BeforeEach
    public void setup() throws IOException {
        this.session = TestUtils.createSession();

        this.columns = new Column[NUM_TABLES][];
        this.rows    = new Row[NUM_TABLES][];
        this.tables  = new Table[NUM_TABLES];

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

        this.props.put(ConnectorUtils.CLUSTER_URI_CONFIG, "qdb://127.0.0.1:28360");
        this.props.put(ConnectorUtils.TABLES_CONFIG, topicMap);
    }

    /**
     * Tests that an exception is thrown when the schema of the key is not a string.
     */
    @ParameterizedTest
    @MethodSource("randomSchemaWithValue")
    public void testPutKeyNull(Schema schema, Object value) {
        this.task.start(this.props);

        List<SinkRecord> records = new ArrayList<SinkRecord>();
        records.add(new SinkRecord(null, -1, schema, value, null, null, -1));

        if (schema != null && schema.type() == Schema.Type.STRING) {
            this.task.put(records);
        } else {
            assertThrows(DataException.class, () -> this.task.put(records));
        }
    }

    static Stream<Arguments> randomSchemaWithValue() {
        return Stream.of(Arguments.of(null, null),
                         Arguments.of(SchemaBuilder.int8(),    (byte)8),
                         Arguments.of(SchemaBuilder.int16(),   (short)16),
                         Arguments.of(SchemaBuilder.int32(),   (int)32),
                         Arguments.of(SchemaBuilder.int64(),   (long)64),
                         Arguments.of(SchemaBuilder.float32(), (float)32.0),
                         Arguments.of(SchemaBuilder.float64(), (double)64.0),
                         Arguments.of(SchemaBuilder.bool(),    true),
                         Arguments.of(SchemaBuilder.string(), "hi, dave"));
    }
}
