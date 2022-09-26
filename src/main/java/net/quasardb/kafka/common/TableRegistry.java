package net.quasardb.kafka.common;

import java.util.Map;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.quasardb.qdb.exception.AliasNotFoundException;
import net.quasardb.qdb.Session;
import net.quasardb.qdb.ts.Table;

/**
 * A registry that contains one or more TableInfo entries, which can be
 * used to lookup and cache metadata about a Table's representation.
 */
public class TableRegistry {
    private static final Logger log = LoggerFactory.getLogger(TableRegistry.class);
    private Map<String, Table> registry;

    public TableRegistry() {
        this.registry = new HashMap<String, Table>();
    }

    /**
     * Dynamically resolve a table and add it to the registry. Blocking
     * function.
     *
     * @param session Active connection with the QuasarDB cluster
     * @param name Table name to look up and add.
     * @return A reference to the added TableInfo object inside the registry, or
     *         null when the table was not found.
     */
    public Table put(Session session, String name) {
        try {
            return this.put(name, new Table(session, name));
        } catch (AliasNotFoundException e) {
            return null;
        }
    }

    public Table put(Table t) {
        return this.put(t.getName(), t);
    }

    public Table put(String name, Table t) {
        this.registry.put(name, t);
        log.debug("Added table " + name + " to TableRegistry");

        return t;
    }

    /**
     * Retrieve a Table by its name, or return null when not found.
     *
     * @param name Name of the table to look up
     * @return Reference to the Tableinfo
     */
    public Table get(String name) {
        return this.registry.get(name);
    }
};
