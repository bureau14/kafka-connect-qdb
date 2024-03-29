package net.quasardb.kafka.sink;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.LoggerFactory;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.errors.DataException;

import net.quasardb.qdb.Logger;
import net.quasardb.qdb.Session;
import net.quasardb.qdb.ts.Column;
import net.quasardb.qdb.ts.Table;
import net.quasardb.qdb.ts.Tables;
import net.quasardb.qdb.ts.Writer;

import net.quasardb.kafka.common.ConnectorUtils;
import net.quasardb.kafka.common.TableRegistry;
import net.quasardb.kafka.common.RecordConverter;
import net.quasardb.kafka.common.resolver.Resolver;
import net.quasardb.kafka.common.writer.RecordWriter;
import net.quasardb.kafka.common.writer.RowRecordWriter;

public class QdbSinkTask extends SinkTask {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(QdbSinkTask.class);
    private static Session _keepAlive = new Session();

    private Session session;
    private Writer writer;

    private TableRegistry tableRegistry;
    private RecordWriter recordWriter;
    private Resolver<String> tableResolver;
    private Resolver<String> skeletonTableResolver;
    private Resolver<List<String>> tableTagsResolver;

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

        this.session =
            ConnectorUtils.connect(validatedProps);

        this.tableResolver = ConnectorUtils.createTableResolver(validatedProps);
        this.skeletonTableResolver = ConnectorUtils.createSkeletonTableResolver(validatedProps);
        this.tableTagsResolver = ConnectorUtils.createTableTagsResolver(validatedProps);
        this.recordWriter = ConnectorUtils.createRecordWriter(validatedProps);

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
            this.tableResolver = null;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Table createTable(String tableName, SinkRecord record) throws DataException {
        // Table not found
        if (this.skeletonTableResolver == null) {
            throw new DataException("Table '" + tableName + "' not found, and no skeleton table configuration for creation, aborting");
        }

        Table skeleton = new Table(this.session, this.skeletonTableResolver.resolve(record));
        log.info("creating copy of skeleton table '" + skeleton.getName() + "' into target table '" + tableName + "'");
        Table newTable = Table.create(this.session, tableName, skeleton);

        if (this.tableTagsResolver != null) {
            List<String> tags = this.tableTagsResolver.resolve(record);
            log.debug("attaching tags " + tags.toString() + " to table " + tableName);
            Table.attachTags(this.session, tableName, tags);
        }

        return newTable;
    }

    private Table addTableToRegistry(String tableName, SinkRecord record) throws DataException {
        if (tableName == null) {
            throw new DataException("Invalid table name provided: " + tableName);
        }

        log.info("Adding table to registry: " + tableName);

        Table t = this.tableRegistry.put(this.session, tableName);
        if (t == null) {
            t = this.tableRegistry.put(this.createTable(tableName, record));
        }

        if (this.writer == null) {
            log.debug("Initializing Writer");
            this.writer = Writer.builder(this.session).asyncPush().dropDuplicates().build();
        }

        return t;
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        for (SinkRecord s : sinkRecords) {
            String tableName = this.tableResolver.resolve(s);
            Table t = this.tableRegistry.get(tableName);

            if (t == null) {
                t = addTableToRegistry(tableName, s);
            }

            this.recordWriter.write(this.writer, t, s);
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
