package net.quasardb.kafka.common;

import java.util.Collection;
import java.util.Map;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.errors.DataException;

public class StaticTableResolver extends TableResolver {

    private static final Logger log = LoggerFactory.getLogger(StaticTableResolver.class);

    private String tableName;

    public StaticTableResolver(String tableName) {
        log.info("Initializing static table resolver for table '" + tableName + "'");
        this.tableName = tableName;
    }


    @Override
    public String resolve(SinkRecord record) throws DataException {
        return this.tableName;
    }

}
