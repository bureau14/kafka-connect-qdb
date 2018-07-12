package net.quasardb.kafka.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.IntStream;
import java.nio.ByteBuffer;
import java.io.IOException;

import org.apache.kafka.connect.data.Field;
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
                                                Value.Type.BLOB };

    private static Session             session;
    private static QdbSinkTask         task;

    private static class Fixture {
        public Column[][]          columns;
        public Row[][]             rows;
        public Table[]             tables;
        public Schema[]            schemas;
        public SinkRecord[][]      records;
        public Map<String, String> props;

        public Fixture() {
            this.columns  = new Column[NUM_TABLES][];
            this.rows     = new Row[NUM_TABLES][];
            this.tables   = new Table[NUM_TABLES];
            this.schemas  = new Schema[NUM_TABLES];
            this.records  = new SinkRecord[NUM_TABLES][];

            this.props    = new HashMap<String, String>();
        }

        public static Fixture of(Session session) throws IOException {
            Fixture out = new Fixture();

            for (int i = 0; i < NUM_TABLES; ++i) {

                // Generate a column of each value type
                out.columns[i] = Arrays.stream(VALUE_TYPES)
                    .map((type) -> {
                            return TestUtils.generateTableColumn(type);
                        })
                    .toArray(Column[]::new);
                out.rows[i] = TestUtils.generateTableRows(out.columns[i], NUM_ROWS);
                out.tables[i] = TestUtils.createTable(session, out.columns[i]);

                // Calculate/determine Kafka Connect representations of the schemas
                out.schemas[i] = TestUtils.columnsToSchema(out.columns[i]);

                final Schema schema = out.schemas[i];
                final String topic  = out.tables[i].getName();

                out.records[i] = Arrays.stream(out.rows[i])
                    .map((row) -> {
                            return TestUtils.rowToRecord(topic, 0, schema, row);
                        })
                    .toArray(SinkRecord[]::new);
            }

            String topicMap = Arrays.stream(out.tables)
                .map((table) -> {
                        // Here we assume kafka topic id == qdb table id
                        return table.getName() + "=" + table.getName();
                    })
                .collect(Collectors.joining(","));


            out.props.put(ConnectorUtils.CLUSTER_URI_CONFIG, "qdb://127.0.0.1:28360");
            out.props.put(ConnectorUtils.TABLES_CONFIG, topicMap);

            return out;
        }
    }

    @BeforeEach
    public void setup() throws IOException {
        this.task = new QdbSinkTask();
    }

    /**
     * Tests that an exception is thrown when the schema of the value is not a struct.
     */
    @ParameterizedTest
    @MethodSource("randomSchemaWithValue")
    public void testPutValuePrimitives(Fixture state, Schema schema, Object value) {
        this.task.start(state.props);

        List<SinkRecord> records = new ArrayList<SinkRecord>();
        records.add(new SinkRecord(null, -1, null, null, schema, value, -1));

        if (schema != null && schema.type() == Schema.Type.STRUCT) {
            this.task.put(records);
        } else {
            assertThrows(DataException.class, () -> this.task.put(records));
        }
    }

    /**
     * Tests that regular rows can be inserted without problem.
     */
    @ParameterizedTest
    @MethodSource("randomRecord")
    public void testPutRow(Fixture fixture, SinkRecord record) {
        this.task.start(fixture.props);

        Collection<SinkRecord> records = Collections.singletonList(record);
        this.task.put(records);
        this.task.stop();
    }

    /**
     * Tests that a collection of regular rows can be inserted without problem.
     */
    @ParameterizedTest
    @MethodSource("randomRecords")
    public void testPutRow(Fixture fixture, Collection<SinkRecord> records) {
        this.task.start(fixture.props);
        this.task.put(records);
    }


    /**
     * Parameter provider for random schemas
     */
    static Stream<Arguments> randomSchemaWithValue() throws IOException {
        Fixture fixture       = Fixture.of(TestUtils.createSession());

        return Stream.of(Arguments.of(fixture, null, null),
                         Arguments.of(fixture, SchemaBuilder.int8(),    (byte)8),
                         Arguments.of(fixture, SchemaBuilder.int16(),   (short)16),
                         Arguments.of(fixture, SchemaBuilder.int32(),   (int)32),
                         Arguments.of(fixture, SchemaBuilder.int64(),   (long)64),
                         Arguments.of(fixture, SchemaBuilder.float32(), (float)32.0),
                         Arguments.of(fixture, SchemaBuilder.float64(), (double)64.0),
                         Arguments.of(fixture, SchemaBuilder.bool(),    true),
                         Arguments.of(fixture, SchemaBuilder.string(), "hi, dave"));
    }

    /**
     * Parameter provider for all rows, grouped per table. Emits exactly
     * NUM_ROWS rows per entry.
     */
    static Stream<Arguments> randomRecords() throws IOException {
        Fixture fixture       = Fixture.of(TestUtils.createSession());

        return IntStream.range(0, fixture.records.length)
            .mapToObj((i) -> {
                    return Arguments.of(fixture, Arrays.asList(fixture.records[i]));
                });
    }

    /**
     * Parameter provider for single row. Emits a max of 10 rows per table.
     */
    static Stream<Arguments> randomRecord() throws IOException {
        final int MAX_ROWS    = 10;
        Fixture fixture       = Fixture.of(TestUtils.createSession());

        return IntStream.range(0, Math.min(MAX_ROWS, fixture.records.length))
            .mapToObj((i) -> {
                    return Arguments.of(fixture.records[i]);
                })
            .flatMap((args) -> {
                    SinkRecord[] r = (SinkRecord[])args.get();
                    return IntStream.range(0, Math.min(MAX_ROWS, r.length))
                        .mapToObj((j) ->
                                  {
                                      return Arguments.of(fixture, r[j]);
                                  });
                });
    }
}
