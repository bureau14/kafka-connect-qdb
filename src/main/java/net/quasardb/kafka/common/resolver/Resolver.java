package net.quasardb.kafka.common.resolver;

import java.util.Collection;
import java.util.Map;
import java.util.HashMap;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.errors.DataException;

abstract public class Resolver<T> {

    abstract public T resolve(SinkRecord record) throws DataException;

}
