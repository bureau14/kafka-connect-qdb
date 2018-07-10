package net.quasardb.kafka.common;

import java.util.*;
import java.nio.ByteBuffer;
import java.util.stream.Stream;
import java.util.function.Supplier;

import java.io.IOException;

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
}
