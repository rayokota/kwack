package io.kcache.kawai.translator.avro;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.kcache.kawai.schema.RelDef;
import io.kcache.kawai.translator.Translator;

public class AvroTranslator implements Translator {
    public ParsedSchema relDefToSchema(RelDef relDef) {
        return null;

    }

    public RelDef schemaToRelDef(ParsedSchema schema) {
        return null;

    }

    public Object rowToMessage(Object row) {
        return null;

    }

    public Object messageToRow(Object message) {
        return null;

    }
}
