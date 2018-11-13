package net.quasardb.kafka.common.resolver;

import java.util.Collection;
import java.util.Map;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.errors.DataException;

public class StaticResolver<T> extends Resolver<T> {

    private static final Logger log = LoggerFactory.getLogger(StaticResolver.class);

    private T value;

    public StaticResolver(T value) {
        log.debug("Initializing static resolver with value '" + value.toString() + "'");
        this.value = value;
    }


    @Override
    public T resolve(SinkRecord record) throws DataException {
        return this.value;
    }

}
