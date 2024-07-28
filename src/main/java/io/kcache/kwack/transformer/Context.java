package io.kcache.kwack.transformer;

import io.kcache.kwack.schema.ColumnDef;
import io.kcache.kwack.schema.UnionColumnDef;
import java.sql.Array;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import org.duckdb.DuckDBConnection;

public class Context {
    private final boolean isKey;
    private final DuckDBConnection conn;
    private final Map<Object, ColumnDef> columnDefs;
    private final Map<UnionColumnDef, String> unionBranches;
    private Object originalMessage;

    public Context(boolean isKey, DuckDBConnection conn) {
        this.isKey = isKey;
        this.conn = conn;
        this.columnDefs = new HashMap<>();
        this.unionBranches = new IdentityHashMap<>();
    }

    public boolean isKey() {
        return isKey;
    }

    public void put(Object key, ColumnDef value) {
        columnDefs.put(key, value);
    }

    public ColumnDef get(Object key) {
        return columnDefs.get(key);
    }

    public void putUnionBranch(UnionColumnDef key, String value) {
        unionBranches.put(key, value);
    }

    public String getUnionBranch(UnionColumnDef key) {
        return unionBranches.get(key);
    }

    public DuckDBConnection getConnection() {
        return conn;
    }

    public Array createArrayOf(String typeName, Object[] attributes) {
        try {
            return conn.createArrayOf(typeName, attributes);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public <K, V> Map<K, V> createMap(String typeName, Map<K, V> map) {
        return conn.createMap(typeName, map);
    }

    public Struct createStruct(String typeName, Object[] attributes) {
        try {
            return conn.createStruct(typeName, attributes);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Object getOriginalMessage() {
        return originalMessage;
    }

    public void setOriginalMessage(Object originalMessage) {
        this.originalMessage = originalMessage;
    }
}
