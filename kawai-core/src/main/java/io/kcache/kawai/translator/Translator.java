package io.kcache.kawai.translator;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.kcache.kawai.schema.RelDef;

public interface Translator {
    ParsedSchema relDefToSchema(Context ctx, RelDef relDef);

    RelDef schemaToRelDef(Context ctx, ParsedSchema schema);

    Object rowToMessage(Context ctx, Object row);

    Object messageToRow(Context ctx, Object message);
}
