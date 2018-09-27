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
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.errors.DataException;

import net.quasardb.qdb.Session;
import net.quasardb.qdb.ts.Column;
import net.quasardb.qdb.ts.Table;
import net.quasardb.qdb.ts.Tables;
import net.quasardb.qdb.ts.Timespec;
import net.quasardb.qdb.ts.Value;
import net.quasardb.qdb.ts.Writer;

import net.quasardb.kafka.codec.Deserializer;
import net.quasardb.kafka.codec.StructDeserializer;
import net.quasardb.kafka.common.ConnectorUtils;
import net.quasardb.kafka.common.TableInfo;
import net.quasardb.kafka.common.TableRegistry;

public class QdbSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(QdbSinkTask.class);

    private Session session;
    private Writer writer;

    private TableRegistry tableRegistry;
    private Map<String, String> topicToTable;
    private Deserializer deserializer;

    /**
     * Always use no-arg constructor, #start will initialize the task.
     */
    public QdbSinkTask() {
    }

    @Override
    public String version() {
        return new QdbSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        if (this.writer != null) {
            throw new RuntimeException("can only start a task once");
        }

        this.tableRegistry = new TableRegistry();

        Map<String, Object> validatedProps = new QdbSinkConnector().config().parse(props);

        this.deserializer = ConnectorUtils.createDeserializer(validatedProps);

        this.session =
            Session.connect((String)validatedProps.get(ConnectorUtils.CLUSTER_URI_CONFIG));

        this.topicToTable = ConnectorUtils.parseTableFromTopic((Collection<String>)validatedProps.get(ConnectorUtils.TABLE_FROM_TOPIC_CONFIG));

        Tables tables = new Tables();
        for (Map.Entry<String, String> entry : this.topicToTable.entrySet()) {
            TableInfo t = this.tableRegistry.put(this.session, entry.getValue());
            tables.add(t.getTable());
        }

        this.writer = Tables.writer(this.session, tables);

        log.info("Started QdbSinkTask");
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

            this.tableRegistry = null;
            this.topicToTable = null;
            this.deserializer = null;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        for (SinkRecord s : sinkRecords) {
            Object parsed = this.deserializer.parse(s);
            String tableName = this.deserializer.tableName(s, parsed);
            TableInfo t = this.tableRegistry.get(tableName);

            if (t == null) {
                throw new DataException("Table not found in registry: " + tableName);
            }

            if (t.hasOffset() == false) {
                t.setOffset(this.writer.tableIndexByName(t.getTable().getName()));
            }

            Value[] row = this.deserializer.convert(t.getTable().getColumns(), parsed);

            try {
                Timespec ts = (s.timestamp() == null
                               ? Timespec.now()
                               : new Timespec(s.timestamp()));
                this.writer.append(t.getOffset(), ts, row);
            } catch (Exception e) {
                log.error("Unable to write record: " + e.getMessage());
                log.error("Record: " + s.toString());
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> partitionOffsets) {
        try {
            if (this.writer != null) {
                log.info("Flush request received, flushing writer..");
                this.writer.flush();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
