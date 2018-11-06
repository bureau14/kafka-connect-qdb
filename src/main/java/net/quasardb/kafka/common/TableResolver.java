package net.quasardb.kafka.common;

import java.util.Collection;
import java.util.Map;
import java.util.HashMap;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.errors.DataException;

abstract public class TableResolver {

    abstract public String resolve(SinkRecord record) throws DataException;

}
