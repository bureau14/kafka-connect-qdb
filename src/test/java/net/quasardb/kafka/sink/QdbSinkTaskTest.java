package net.quasardb.kafka.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.AfterEach;
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
import java.util.ArrayList;
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

import net.quasardb.qdb.ts.Column;
import net.quasardb.qdb.ts.Reader;
import net.quasardb.qdb.ts.Row;
import net.quasardb.qdb.ts.Value;
import net.quasardb.qdb.ts.TimeRange;
import net.quasardb.qdb.ts.Timespec;
import net.quasardb.qdb.ts.Table;
import net.quasardb.qdb.ts.Tables;

import net.quasardb.kafka.common.ConnectorUtils;
import net.quasardb.kafka.common.TestUtils;
import net.quasardb.kafka.common.Fixture;

public class QdbSinkTaskTest {

    private static final int     NUM_TASKS    = 10;
    private static Schema.Type[] SCHEMA_TYPES = { Schema.Type.STRING, Schema.Type.STRUCT };
    private static QdbSinkTask   task;

    @BeforeEach
    public void setup() {
        this.task = new QdbSinkTask();
    }

    @AfterEach
    public void teardown() {
        System.gc();
    }

    /**
     * Tests that an exception is thrown when the schema of the value is not a struct.
     */
    @ParameterizedTest
    @MethodSource("randomSchemaWithValue")
    public void testPutValuePrimitives(Fixture state, Schema schema, Object value) {
        Map<String, String> props = state.props;
        props.put(ConnectorUtils.TABLE_FROM_TOPIC_CONFIG, "true");

        this.task.start(props);

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
    public void testPutRow(Fixture fixture, Row row, SinkRecord record) {
        Map<String, String> props = fixture.props;
        props.put(ConnectorUtils.TABLE_FROM_TOPIC_CONFIG, "true");

        this.task.start(props);
        this.task.put(Collections.singletonList(record));
        this.task.stop();
    }

    /**
     * Tests that a collection of regular rows can be inserted without problem.
     */
    @ParameterizedTest
    @MethodSource("randomRecords")
    public void testPutRows(Fixture fixture, Collection<SinkRecord> records) {
        Map<String, String> props = fixture.props;
        props.put(ConnectorUtils.TABLE_FROM_TOPIC_CONFIG, "true");

        this.task.start(props);
        this.task.put(records);
        this.task.stop();
    }

    /**
     * Tests that rows are visible after flushing.
     */
    @ParameterizedTest
    @MethodSource("randomRecord")
    public void testRowsVisibleAfterFlush(Fixture fixture, Row row, SinkRecord record) {
        Map<String, String> props = fixture.props;
        props.put(ConnectorUtils.TABLE_FROM_TOPIC_CONFIG, "true");

        this.task.start(props);
        this.task.put(Collections.singletonList(record));
        this.task.flush(new HashMap<>());

        String tableName = record.topic();

        Timespec ts = new Timespec(record.timestamp());
        TimeRange[] ranges = { new TimeRange(ts, ts.plusNanos(1)) };

        Reader reader = Table.reader(TestUtils.createSession(), tableName, ranges);

        assertEquals(true, reader.hasNext());

        Row row2 = reader.next();

        assertEquals(row, row2);
        assertEquals(false, reader.hasNext());

        this.task.stop();
    }

    /**
     * Tests that a new table can be created by a skeleton.
     */

    @ParameterizedTest
    @MethodSource("randomRecord")
    public void testAutoCreateTable(Fixture fixture, Row row, SinkRecord record) {
        Map<String, String> props = fixture.props;
        props.put(ConnectorUtils.TABLE_CONFIG, record.topic());

        this.task.start(props);
        this.task.put(Collections.singletonList(record));
        this.task.stop();
    }

    /**
     * Parameter provider for random schemas
     */
    static Stream<Arguments> randomSchemaWithValue() throws IOException {
        Fixture fixture       = Fixture.of(TestUtils.createSession()).withRecords(Schema.Type.STRUCT);

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
        return (IntStream.range(0, SCHEMA_TYPES.length))
            .mapToObj((i) -> {
                    try {
                        return Fixture.of(TestUtils.createSession()).withRecords(SCHEMA_TYPES[i]);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
            .flatMap((fixture) -> {
                    return IntStream.range(0, fixture.records.length)
                        .mapToObj((i) -> {
                                return Arguments.of(fixture, Arrays.asList(fixture.records[i]));
                            });
                        });
    }

    /**
     * Parameter provider for single row. Emits a max of 10 rows per table.
     */
    static Stream<Arguments> randomRecord() throws IOException {
        final int MAX_ROWS    = 10;

        // For each struct type, generate a fixture that holds schema/records
        // of that type.
        return (IntStream.range(0, SCHEMA_TYPES.length))
            .mapToObj((i) -> {
                    try {
                        return Fixture.of(TestUtils.createSession()).withRecords(SCHEMA_TYPES[i]);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
            .flatMap((fixture) -> {
                    return IntStream.range(0, Math.min(MAX_ROWS, fixture.records.length))
                        .mapToObj((i) -> {
                                return Arguments.of(fixture.rows[i], fixture.records[i]);
                            })
                        .flatMap((args) -> {
                                Row[] rows = (Row[])args.get()[0];
                                SinkRecord[] records = (SinkRecord[])args.get()[1];
                                return IntStream.range(0, Math.min(MAX_ROWS, records.length))
                                    .mapToObj((j) ->
                                              {
                                                  return Arguments.of(fixture, rows[j], records[j]);
                                              });
                            });
                        });
    }
}
