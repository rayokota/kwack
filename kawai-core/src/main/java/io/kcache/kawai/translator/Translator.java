package io.kcache.kawai.translator;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.kcache.kawai.schema.RelDef;

public interface Translator {
    ParsedSchema relDefToSchema(Context ctx, RelDef relDef);

    RelDef schemaToRelDef(Context ctx, ParsedSchema parsedSchema);

    Object rowToMessage(Context ctx, RelDef relDef, Object row, ParsedSchema parsedSchema);

    Object messageToRow(Context ctx, ParsedSchema parsedSchema, Object message, RelDef relDef);
}
