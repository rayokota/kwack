package io.kcache.kwack.loader;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.kcache.kwack.schema.ColumnDef;

public interface Loader {
    ColumnDef schemaToColumnDef(Context ctx, ParsedSchema parsedSchema);

    Object messageToColumn(
        Context ctx, ParsedSchema parsedSchema, Object message, ColumnDef columnDef);
}
