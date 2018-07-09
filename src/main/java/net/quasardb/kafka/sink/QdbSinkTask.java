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
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

public class QdbSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(QdbSinkTask.class);

    public QdbSinkTask() {}

    @Override
    public String version() {
        return new QdbSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
    }

    @Override
    public void stop() {
        // TODO implement
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> partitionOffsets) {
        // TODO implement
    }

}
