package io.kcache.kawai.translator;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.kcache.kawai.schema.RelDef;

public interface Translator {
    ParsedSchema relDefToSchema(RelDef relDef);

    RelDef schemaToRelDef(ParsedSchema schema);

    Object rowToMessage(Object row);

    Object messageToRow(Object message);
}
