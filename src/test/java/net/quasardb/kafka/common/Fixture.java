package net.quasardb.kafka.common;

import java.io.IOException;

import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import net.quasardb.qdb.Session;
import net.quasardb.qdb.ts.Column;
import net.quasardb.qdb.ts.Row;
import net.quasardb.qdb.ts.Table;
import net.quasardb.qdb.ts.Value;


public class Fixture {
    private static final int    NUM_TABLES  = 4;
    private static final int    NUM_ROWS    = 1000;
    private static Value.Type[] VALUE_TYPES = { Value.Type.INT64,
                                                Value.Type.DOUBLE,
                                                Value.Type.BLOB };


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
