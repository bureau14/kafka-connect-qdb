package net.quasardb.kafka.sink;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.errors.DataException;

import net.quasardb.qdb.Session;
import net.quasardb.qdb.ts.Table;
import net.quasardb.qdb.ts.Tables;
import net.quasardb.qdb.ts.Value;
import net.quasardb.qdb.ts.Writer;
import net.quasardb.kafka.common.ConnectorUtils;

public class QdbSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(QdbSinkTask.class);

    private Session session;
    private Writer writer;

    private Map<String, TableInfo> topicToTable;

    static private class TableInfo {
        private Table table;
        private int offset;

        public TableInfo(Table table) {
            this.table = table;
            this.offset = -1;
        }

        public TableInfo(Table table, int offset) {
            this.table = table;
            this.offset = offset;
        }

        public boolean hasOffset() {
            return this.offset != -1;
        }

        public void setOffset(int offset) {
            this.offset = offset;
        }

        public int getOffset() {
            return this.offset;
        }

        public Table getTable() {
            return this.table;
        }

        public String toString() {
            return "TableInfo (table: " + this.table.toString() + ", offset: " + this.offset + ")";
        }
    };

    /**
     * Always use no-arg constructor, #start will initialize the task.
     */
    public QdbSinkTask() {}

    @Override
    public String version() {
        return new QdbSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        if (this.writer != null) {
            throw new RuntimeException("can only start a task once");
        }

        Map<String, Object> validatedProps = new QdbSinkConnector().config().parse(props);
        this.session =
            Session.connect((String)validatedProps.get(ConnectorUtils.CLUSTER_URI_CONFIG));

        this.topicToTable =
           this.resolveTableInfo(this.session,
                                 (Collection<String>)validatedProps.get(ConnectorUtils.TABLES_CONFIG));

        Table[] tables =
            this.topicToTable.entrySet().stream()
            .map((x) -> {
                    return x.getValue().getTable();
                })
            .toArray(Table[]::new);

        this.writer = Tables.autoFlushWriter(this.session, tables);

        log.info("Started QdbSinkTask");
    }

    /**
     * Takes a validated table configuration, and resolve the actual Qdb table information
     * based on it.
     */
    static private Map<String, TableInfo> resolveTableInfo(Session session,
                                                           Collection<String> config) {
        Map<String, String> parsedConfig = ConnectorUtils.parseTablesConfig(config);
        Map<String, TableInfo> out = new HashMap<String, TableInfo>();

        for (Map.Entry<String, String> entry : parsedConfig.entrySet()) {
            Table table = new Table(session, entry.getValue());
            out.put(entry.getKey(), new TableInfo(table));
        }

        return out;
    }

    @Override
    public void stop() {
        log.info("Stopping QdbSinkTask");

        try {
            if (this.writer != null) {
                this.writer.close();
                this.writer = null;
            }

            if (this.session != null) {
                this.session.close();
                this.session = null;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        for (SinkRecord s : sinkRecords) {
            if (s.valueSchema() == null ||
                s.valueSchema().type() != Schema.Type.STRUCT) {
                throw new DataException("Only Struct values are supported, got: " + s.valueSchema());
            }

            TableInfo t = tableFromRecord(this.topicToTable, s);
            Value[] row = recordToValue((Struct)s.value());
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> partitionOffsets) {
        // TODO implement
    }

    private static Value[] recordToValue(Struct record) {
        return null;
    }

    /**
     * Looks up the appropriate Table based on a Record. In future version, this can
     * also contain logic to dynamically resolve a table from a record's struct's field
     * value.
     *
     * @param topicToTable Hardcoded mapping of kafka topic to qdb table.
     * @param record The Kafka record being processed.
     */
    private static TableInfo tableFromRecord(Map<String, TableInfo> topicToTable, SinkRecord record) throws DataException {

        log.debug("1 tableFromRecord, topicToTable keys = " + topicToTable.keySet().toString());

        TableInfo t = topicToTable.get(record.topic());

        log.debug("tableFromRecord, t = " + t);

        if (t == null) {
            log.error("Topic for record not found: " + record.topic());
            log.error("If this problem persists, please restart the connector with an " +
                      "appropriate mapping of this topic to a QuasarDB table.");
            throw new DataException("Unexpected topic, unable to map to QuasarDB table");
        }
        return null;
    }
}
