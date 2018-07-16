package net.quasardb.kafka.common;

import net.quasardb.qdb.ts.Table;

/**
 * Keeps track of Table and metadata we collect about the table.
 */
public class TableInfo {
    private Table table;
    private int offset;

    public TableInfo(Table table) {
        this.table = table;
        this.offset = -1;
    }

    public TableInfo(Table table, int offset) {
        this.table = table;
        this.offset = offset;
    }

    public boolean hasOffset() {
        return this.offset != -1;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getOffset() {
        return this.offset;
    }

    public Table getTable() {
        return this.table;
    }

    public String toString() {
        return "TableInfo (table: " + this.table.toString() + ", offset: " + this.offset + ")";
    }
};
