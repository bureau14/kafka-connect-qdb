package net.quasardb.kafka.common;

import java.util.Collection;
import java.util.Map;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.errors.DataException;

public class TopicTableResolver extends TableResolver {

    private static final Logger log = LoggerFactory.getLogger(StaticTableResolver.class);

    public TopicTableResolver() {
        log.info("Initializing topic table resolver");
    }


    @Override
    public String resolve(SinkRecord record) throws DataException {
        return record.topic();
    }

}
