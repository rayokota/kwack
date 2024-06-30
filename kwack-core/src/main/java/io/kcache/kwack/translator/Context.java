package io.kcache.kwack.translator;

import java.sql.Array;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.Map;
import org.duckdb.DuckDBConnection;

public class Context {
    private final boolean isKey;
    private DuckDBConnection connection;

    public Context(boolean isKey) {
        this.isKey = isKey;
    }

    public DuckDBConnection getConnection() {
        return connection;
    }

    public Array createArrayOf(String typeName, Object[] attributes) {
        try {
            return connection.createArrayOf(typeName, attributes);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public <K, V> Map<K, V> createMap(String typeName, Map<K, V> map) {
        throw new UnsupportedOperationException();
        // TODO requires DuckDB 1.1.0
        /*
        try {
            return new connection.createMap(typeName, map);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        */
    }

    public Struct createStruct(String typeName, Object[] attributes) {
        try {
            return connection.createStruct(typeName, attributes);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
