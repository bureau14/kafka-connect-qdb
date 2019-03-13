package net.quasardb.kafka.common.resolver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringColumnResolver extends ColumnResolver<String> {

    private static final Logger log = LoggerFactory.getLogger(StringColumnResolver.class);

    public StringColumnResolver(String columnName) {
        super(columnName);
    }

    public StringColumnResolver(String columnName, String suffix) {
        super(columnName, suffix);
    }

    protected String handleSuffix(String result, String suffix) {
        this.log.info("Handling string suffix");
        return result + suffix;
    }
}
