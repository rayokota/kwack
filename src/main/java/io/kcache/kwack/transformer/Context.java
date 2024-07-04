package io.kcache.kwack.transformer;

import io.kcache.kwack.schema.ColumnDef;
import java.sql.Array;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.HashMap;
import java.util.Map;
import org.duckdb.DuckDBConnection;

public class Context {
    private final boolean isKey;
    private final DuckDBConnection conn;
    private final Map<Object, ColumnDef> cache;

    public Context(boolean isKey, DuckDBConnection conn) {
        this.isKey = isKey;
        this.conn = conn;
        this.cache = new HashMap<>();
    }

    public boolean isKey() {
        return isKey;
    }

    public void put(Object key, ColumnDef value) {
        cache.put(key, value);
    }

    public ColumnDef get(Object key) {
        return cache.get(key);
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
}
