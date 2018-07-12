package net.quasardb.kafka.common;

import java.util.*;
import java.nio.ByteBuffer;
import java.util.stream.Stream;
import java.util.function.Supplier;
import java.io.IOException;

import org.apache.kafka.common.record.TimestampType;
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

public class TestUtils {

    private static long n = 1;

    public static Session createSession() {
        return Session.connect("qdb://127.0.0.1:28360");
    }

    public static Column[] generateTableColumns(int count) {
        return generateTableColumns(Value.Type.DOUBLE, count);
    }

    public static Column generateTableColumn(Value.Type valueType) {
        return generateTableColumns(valueType, 1)[0];
    }

    public static Column[] generateTableColumns(Value.Type valueType, int count) {
        return Stream.generate(TestUtils::createUniqueAlias)
            .limit(count)
            .map((alias) -> {
                    return new Column(alias, valueType);
                })
            .toArray(Column[]::new);
    }

    /**
     * Creates a new QuasarDB table based on a column layout. Assigns a random
     * name to it. Uses default shard size.
     */
    public static Table createTable(Session session,
                                    Column[] columns) throws IOException {
        return Table.create(session,
                            createUniqueAlias(),
                            columns);
    }

    public static Value generateRandomValueByType(int complexity, Value.Type valueType) {
        switch (valueType) {
        case INT64:
            return Value.createInt64(randomInt64());
        case DOUBLE:
            return Value.createDouble(randomDouble());
        case TIMESTAMP:
            return Value.createTimestamp(randomTimestamp());
        case BLOB:
            return Value.createSafeBlob(createSampleData(complexity));
        }

        return Value.createNull();

    }

    /**
     * Generate table rows with standard complexity of 32
     */
    public static Row[] generateTableRows(Column[] cols, int count) {
        return generateTableRows(cols, 32, count);
    }

    /**
     * Generate table rows.
     *
     * @param cols       Describes the table layout
     * @param complexity Arbitrary complexity variable that is used when generating data.
     *                   E.g. for blobs, this denotes the size of the blob value being
     *                   generated.
     */
    public static Row[] generateTableRows(Column[] cols, int complexity, int count) {
        // Generate that returns entire rows with an appropriate value for each column.
        Supplier<Value[]> valueGen =
            (() ->
             Arrays.stream(cols)
             .map(Column::getType)
             .map((Value.Type valueType) -> {
                     return TestUtils.generateRandomValueByType(complexity, valueType);
                 })
             .toArray(Value[]::new));


        return Stream.generate(valueGen)
            .limit(count)
            .map((v) ->
                 new Row(Timespec.now(),
                         v))
            .toArray(Row[]::new);
    }


    public static ByteBuffer createSampleData() {
        return createSampleData(32);
    }

    public static ByteBuffer createSampleData(int size) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(size);
        createSampleData(size, buffer);
        return buffer;
    }

    public static void createSampleData(int size, ByteBuffer buffer) {
        byte[] b = new byte[size];
        createSampleData(b);

        buffer.put(b);
        buffer.flip();
    }

    public static void createSampleData(byte[] b) {
        new Random(n++).nextBytes(b);
    }

    public static double randomDouble() {
        return new Random(n++).nextDouble();
    }

    public static long randomInt64() {
        return new Random(n++).nextLong();
    }

    public static Timespec randomTimespec() {
        return randomTimestamp();

    }

    public static Timespec randomTimestamp() {
        return new Timespec(new Random(n++).nextInt(),
                            new Random(n++).nextInt());

    }

    public static String createUniqueAlias() {
        return new String("a") + UUID.randomUUID().toString().replaceAll("-", "");
    }

    /**
     * Utility function to convert a QuasarDB row to a Kafka record.
     */
    public static SinkRecord rowToRecord(String topic,
                                         Integer partition,
                                         Schema schema,
                                         Row row) {
        return rowToRecord(topic, partition, schema, row.getTimestamp(), row.getValues());
    }

    /**
     * Utility function to convert a QuasarDB row to a Kafka record.
     */
    public static SinkRecord rowToRecord(String topic,
                                         Integer partition,
                                         Schema schema,
                                         Timespec time,
                                         Value[] row) {
        Struct value = new Struct(schema);

        Field[] fields = schema.fields().toArray(new Field[schema.fields().size()]);

        // In these tests, we're using exactly one kafka schema field for every
        // field in our rows. These schemas are always the same for all rows, and
        // we're not testing ommitted fields.
        for(int i = 0; i < fields.length; ++i) {
            switch (row[i].getType()) {
            case INT64:
                value.put(fields[i], row[i].getInt64());
                break;
            case DOUBLE:
                value.put(fields[i], row[i].getDouble());
                break;
            case BLOB:
                ByteBuffer bb = row[i].getBlob();
                int size = bb.capacity();
                byte[] buffer = new byte[size];
                bb.get(buffer, 0, size);
                bb.rewind();
                value.put(fields[i], buffer);
                break;
            default:
                throw new DataException("row field type not supported: " + value.toString());
            }
        }

        return new SinkRecord(topic, partition,
                              null, null,    // key is unused
                              schema, value,
                              -1,            // kafkaOffset
                              time.toEpochMillis(), TimestampType.CREATE_TIME);
    }

    /**
     * Utility function to convert a QuasarDB table definition to a Kafka Schema
     */
    public static Schema columnsToSchema(Column[] columns) {
        SchemaBuilder builder = SchemaBuilder.struct();

        for (Column c : columns) {
            switch (c.getType()) {
            case INT64:
                builder.field(c.getName(), SchemaBuilder.int64());
                break;
            case DOUBLE:
                builder.field(c.getName(), SchemaBuilder.float64());
                break;
            case BLOB:
                builder.field(c.getName(), SchemaBuilder.bytes());
                break;
            default:
                throw new DataException("column field type not supported: " + c.toString());
            }
        }

        return builder.build();
    }
}
