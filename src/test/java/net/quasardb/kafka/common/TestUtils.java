package net.quasardb.kafka.common;

import java.util.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.stream.Stream;
import com.google.common.collect.Streams;
import java.util.function.Supplier;
import java.io.IOException;
import java.io.Writer;
import java.io.StringWriter;
import java.time.Instant;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.errors.DataException;
import net.quasardb.qdb.Session;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonEncoding;

import net.quasardb.qdb.ts.Column;
import net.quasardb.qdb.ts.Row;
import net.quasardb.qdb.ts.Value;
import net.quasardb.qdb.ts.Timespec;
import net.quasardb.qdb.ts.Table;

public class TestUtils {
    public static final String CLUSTER_URI = "qdb://127.0.0.1:28361";
    public static final String SECURITY_USERNAME = "qdb-kafka-connector";
    public static final String SECURITY_USER_PRIVATE_KEY = "SoHHpH26NtZvfq5pqm/8BXKbVIkf+yYiVZ5fQbq1nbcI=";
    public static final String SECURITY_CLUSTER_PUBLIC_KEY = "Pb+d1o3HuFtxEb5uTl9peU89ze9BZTK9f8KdKr4k7zGA=";

    private static long n = 1;
    private static JsonFactory jsonFactory = new JsonFactory();

    public static Session createSession() {
        return Session.connect(new Session.SecurityOptions(SECURITY_USERNAME,
                                                           SECURITY_USER_PRIVATE_KEY,
                                                           SECURITY_CLUSTER_PUBLIC_KEY),
                               CLUSTER_URI);
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
            return Value.createSafeString(randomString());
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




        return
            Streams.mapWithIndex(Stream.generate(valueGen),

                                 // We are using millisecond-based timestamps on purpose,
                                 // because Kafka rounds to milliseconds on purpose.
                                 //
                                 // Adding one second to each row ensure each row has a
                                 // unique ts
                                 (v, i) -> new Row(new Timespec(Instant.now().toEpochMilli()).plusSeconds(i),

                                                   v))
            .limit(count)
            .toArray(Row[]::new);
    }

    public static String randomString() {
        return UUID.randomUUID().toString();
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
                                         Table skeletonTable,
                                         String skeletonColumnId,
                                         String tableName,
                                         String tableColumnId,
                                         Schema schema,
                                         List<String> tags,
                                         String tagsColumnId,
                                         Column[] columns,
                                         Row row) throws IOException  {
        return rowToRecord(topic, partition,
                           skeletonTable, skeletonColumnId,
                           tableName, tableColumnId,
                           schema,
                           tags, tagsColumnId,
                           columns, row.getTimestamp(), row.getValues());
    }

    /**
     * Utility function to convert a QuasarDB row to a Kafka record.
     */
    public static SinkRecord rowToRecord(String topic,
                                         Integer partition,
                                         Table skeletonTable,
                                         String skeletonColumnId,
                                         String tableName,
                                         String tableColumnId,
                                         Schema schema,
                                         List<String> tags,
                                         String tagsColumnId,
                                         Column[] columns,
                                         Timespec time,
                                         Value[] row) throws IOException  {
        Object value = null;

        if (schema != null) {
            switch (schema.type()) {
            case STRUCT:
                value = rowToStructValue(schema,
                                         skeletonTable, skeletonColumnId,
                                         tableName, tableColumnId,
                                         tags, tagsColumnId,
                                         row);
                break;
            }

        } else {
            // Schemaless JSON, so we need to pass our columns
            value = rowToMap(columns,
                             skeletonTable, skeletonColumnId,
                             tableName, tableColumnId,
                             tags, tagsColumnId,
                             row);
        }


        return new SinkRecord(topic, partition,
                              null, null,    // key is unused
                              schema, value,
                              -1,            // kafkaOffset
                              time.toEpochMillis(), TimestampType.CREATE_TIME);

    }

    private static Map rowToMap(Column[] columns,
                                Table skeletonTable,
                                String skeletonColumnId,
                                String tableName,
                                String tableColumnId,
                                List<String> tags,
                                String tagsColumnId,
                                Value[] row) {
        Map out = new HashMap<String, Object>();

        for (int i = 0; i < columns.length; ++i) {
            switch (columns[i].getType()) {
            case INT64:
                out.put(columns[i].getName(), row[i].getInt64());
                break;
            case DOUBLE:
                out.put(columns[i].getName(), row[i].getDouble());
                break;
            case BLOB:
                ByteBuffer bb = row[i].getBlob();
                int size = bb.capacity();
                byte[] buffer = new byte[size];
                bb.get(buffer, 0, size);
                bb.rewind();
                out.put(columns[i].getName(), new String(buffer));
                break;
            }
        }

        out.put(skeletonColumnId, skeletonTable.getName());
        out.put(tableColumnId, tableName);
        out.put(tagsColumnId, tags);

        return out;
    }

    public static Struct rowToStructValue(Schema schema,
                                          Table skeletonTable,
                                          String skeletonColumnId,
                                          String tableName,
                                          String tableColumnId,
                                          List<String> tags,
                                          String tagsColumnId,
                                          Value[] row) {
        Struct value = new Struct(schema);

        Field[] fields = schema.fields().toArray(new Field[schema.fields().size()]);

        // In these tests, we're using exactly one kafka schema field for every
        // field in our rows. These schemas are always the same for all rows, and
        // we're not testing ommitted fields.
        for(int i = 0; i < fields.length; ++i) {
            if (fields[i].name() == skeletonColumnId) {
                value.put(skeletonColumnId, skeletonTable.getName());
            } else if (fields[i].name() == tableColumnId) {
                value.put(tableColumnId, tableName);
            } else if (fields[i].name() == tagsColumnId) {
                value.put(tagsColumnId, tags);
            } else {
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
        }

        return value;
    }

    public static String rowToJsonValue(Column[] columns, Value[] row) throws IOException {
        Writer out = new StringWriter();

        JsonGenerator gen = jsonFactory.createJsonGenerator(out);
        gen.writeStartObject();

        for (int i = 0; i < columns.length; ++i) {
            switch (columns[i].getType()) {
            case INT64:
                gen.writeNumberField(columns[i].getName(), row[i].getInt64());
                break;
            case DOUBLE:
                gen.writeNumberField(columns[i].getName(), row[i].getDouble());
                break;
            case BLOB:
                ByteBuffer bb = row[i].getBlob();
                int size = bb.capacity();
                byte[] buffer = new byte[size];
                bb.get(buffer, 0, size);
                bb.rewind();
                gen.writeBinaryField(columns[i].getName(), buffer);
                break;
            default:
                throw new DataException("row field type not supported: " + columns[i].toString());
            }
        }


        gen.writeEndObject();
        gen.close();

        return out.toString();
    }

    /**
     * Utility function to convert a QuasarDB table definition to a Kafka Schema.
     *
     * @param schemaType Type of schema to render. Currently only Schema.Type.STRUCT
     *                   and Schema.Type.String (JSON) are supported.
     *
     */
    public static Schema columnsToSchema(Schema.Type schemaType,
                                         String skeletonColumnId,
                                         String tableColumnId,
                                         String tagsColumnId,
                                         Column[] columns) {
        SchemaBuilder builder = new SchemaBuilder(schemaType);
        switch (schemaType) {
        case STRUCT:
            columnsToStructSchema(builder, skeletonColumnId, tableColumnId, tagsColumnId, columns);
            break;

        case STRING:
            // Only supporting schemaless JSON for now, not doing anything
            return null;

        }

        return builder.build();
    }


    /**
     * Converts a QuasarDB table definition to a Kafka struct-based schema.
     */
    public static void columnsToStructSchema(SchemaBuilder builder,
                                             String skeletonColumnId,
                                             String tableColumnId,
                                             String tagsColumnId,
                                             Column[] columns) {
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

        builder.field(skeletonColumnId, SchemaBuilder.string());
        builder.field(tableColumnId, SchemaBuilder.string());
        builder.field(tagsColumnId, SchemaBuilder.array(Schema.STRING_SCHEMA));
    }
}
