package io.kcache.kwack.transformer;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.kcache.kwack.schema.ColumnDef;

public interface Transformer {
    ColumnDef schemaToColumnDef(Context ctx, ParsedSchema parsedSchema);

    Object messageToColumn(
        Context ctx, ParsedSchema parsedSchema, Object message, ColumnDef columnDef);
}
