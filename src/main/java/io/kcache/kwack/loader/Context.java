package io.kcache.kwack.loader;

import java.sql.Array;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.Map;
import org.duckdb.DuckDBConnection;

public class Context {
    private final boolean isKey;
    private final DuckDBConnection conn;

    public Context(boolean isKey, DuckDBConnection conn) {
        this.isKey = isKey;
        this.conn = conn;
    }

    public boolean isKey() {
        return isKey;
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
