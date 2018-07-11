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

    private class TableInfo {
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

        /**
         * We need to resolve some relevant metadata based on the configuration. We
         * need to look up all the tables, create a Writer out of them, and once we have
         * the writer, we can pre-cache all the tables' offsets within the bulk write.
         */
        Map<String, String> tableConfig =  ConnectorUtils.parseTablesConfig((Collection<String>)validatedProps.get(ConnectorUtils.TABLES_CONFIG));


        Tables tables = new Tables();
        this.topicToTable = new HashMap<String, TableInfo>();

        for (Map.Entry<String, String> entry : tableConfig.entrySet()) {
            Table table = new Table(this.session, entry.getValue());
            tables.add(table);
            this.topicToTable.put(entry.getKey(), new TableInfo(table));
        }

        this.writer = Tables.autoFlushWriter(this.session, tables);

        log.info("Started QdbSinkTask, table mapping: " + this.topicToTable);
    }

    @Override
    public void stop() {
        log.info("Stopping QdbSinkTask");

        if (this.session != null) {
            this.session.close();
        }

        this.session = null;
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        for (SinkRecord s : sinkRecords) {
            if (s.valueSchema() == null ||
                s.valueSchema().type() != Schema.Type.STRUCT) {
                throw new DataException("Only Struct values are supported, got: " + s.valueSchema());
            }

            //Table t = tableFromRecord(this.session, this.topicToTable, s);
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


}
