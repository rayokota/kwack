package io.kcache.kwack.translator;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.kcache.kwack.schema.ColumnDef;

public interface Translator {
    ParsedSchema columnDefToSchema(Context ctx, ColumnDef columnDef);

    ColumnDef schemaToColumnDef(Context ctx, ParsedSchema parsedSchema);

    Object columnToMessage(Context ctx, ColumnDef columnDef, Object row, ParsedSchema parsedSchema);

    Object messageToColumn(
        Context ctx, ParsedSchema parsedSchema, Object message, ColumnDef columnDef);
}
