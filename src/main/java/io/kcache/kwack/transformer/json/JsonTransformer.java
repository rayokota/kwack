package io.kcache.kwack.transformer.json;

import static io.kcache.kwack.schema.ColumnStrategy.NOT_NULL_STRATEGY;
import static io.kcache.kwack.schema.ColumnStrategy.NULL_STRATEGY;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.kcache.kwack.schema.ColumnDef;
import io.kcache.kwack.schema.EnumColumnDef;
import io.kcache.kwack.schema.ListColumnDef;
import io.kcache.kwack.schema.MapColumnDef;
import io.kcache.kwack.schema.StructColumnDef;
import io.kcache.kwack.schema.UnionColumnDef;
import io.kcache.kwack.transformer.Context;
import io.kcache.kwack.transformer.Transformer;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.duckdb.DuckDBColumnType;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.ConstSchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;

public class JsonTransformer implements Transformer {
    @Override
    public ColumnDef schemaToColumnDef(Context ctx, ParsedSchema parsedSchema) {
        Schema schema = (Schema) parsedSchema.rawSchema();
        return schemaToColumnDef(ctx, schema);
    }

    private ColumnDef schemaToColumnDef(Context ctx, Schema schema) {
        LinkedHashMap<String, ColumnDef> columnDefs = new LinkedHashMap<>();
        if (schema instanceof BooleanSchema) {
            return new ColumnDef(DuckDBColumnType.BOOLEAN);
        } else if (schema instanceof NumberSchema) {
            NumberSchema numberSchema = (NumberSchema) schema;
            return new ColumnDef(numberSchema.requiresInteger()
                ? DuckDBColumnType.BIGINT
                : DuckDBColumnType.DOUBLE);
        } else if (schema instanceof StringSchema) {
            return new ColumnDef(DuckDBColumnType.VARCHAR);
        } else if (schema instanceof ConstSchema) {
            ConstSchema constSchema = (ConstSchema) schema;
            return new EnumColumnDef(
                Collections.singletonList(constSchema.getPermittedValue().toString()));
        } else if (schema instanceof EnumSchema) {
            EnumSchema enumSchema = (EnumSchema) schema;
            return new EnumColumnDef(enumSchema.getPossibleValues().stream()
                .map(Object::toString).collect(Collectors.toList()));
        } else if (schema instanceof CombinedSchema) {
            CombinedSchema combinedSchema = (CombinedSchema) schema;
            Schema singletonUnion = flattenSingletonUnion(combinedSchema);
            if (singletonUnion != null) {
                ColumnDef colDef = schemaToColumnDef(ctx, singletonUnion);
                if (combinedSchema.getSubschemas().size() > 1) {
                    colDef.setColumnStrategy(NULL_STRATEGY);
                }
                return colDef;
            }
            CombinedSchema.ValidationCriterion criterion = combinedSchema.getCriterion();
            if (criterion == CombinedSchema.ALL_CRITERION) {
                return allOfToConnectSchema(ctx, combinedSchema)._2;
            }
            int i = 0;
            boolean nullable = false;
            for (Schema subSchema : combinedSchema.getSubschemas()) {
                if (subSchema instanceof NullSchema) {
                    nullable = true;
                } else {
                    columnDefs.put("u" + i, schemaToColumnDef(ctx, subSchema));
                }
                i++;
            }
            return new UnionColumnDef(columnDefs, nullable
                ? NULL_STRATEGY
                : NOT_NULL_STRATEGY);
        } else if (schema instanceof ArraySchema) {
            ArraySchema arraySchema = (ArraySchema) schema;
            return new ListColumnDef(schemaToColumnDef(ctx, arraySchema.getAllItemSchema()));
        } else if (schema instanceof ObjectSchema) {
            ObjectSchema objectSchema = (ObjectSchema) schema;
            if (objectSchema.getSchemaOfAdditionalProperties() != null) {
                // mbknor uses schema of additionalProperties to represent a map
                return new MapColumnDef(new ColumnDef(DuckDBColumnType.VARCHAR),
                    schemaToColumnDef(ctx, objectSchema.getSchemaOfAdditionalProperties()));
            }
            Map<String, Schema> properties = objectSchema.getPropertySchemas();
            StructColumnDef structColumnDef = new StructColumnDef(columnDefs);
            ctx.put(schema, structColumnDef);
            for (Map.Entry<String, Schema> entry : properties.entrySet()) {
                columnDefs.put(entry.getKey(), schemaToColumnDef(ctx, entry.getValue()));
            }
            return structColumnDef;
        } else if (schema instanceof ReferenceSchema) {
            ReferenceSchema referenceSchema = (ReferenceSchema) schema;
            Schema referredSchema = referenceSchema.getReferredSchema();
            ColumnDef columnDef = ctx.get(referredSchema);
            if (columnDef != null) {
                return columnDef;
            }
            return schemaToColumnDef(ctx, referredSchema);
        }
        return null;
    }

    private Tuple2<Schema, ColumnDef> allOfToConnectSchema(Context ctx, CombinedSchema combinedSchema) {
        ConstSchema constSchema = null;
        EnumSchema enumSchema = null;
        NumberSchema numberSchema = null;
        StringSchema stringSchema = null;
        CombinedSchema combinedSubschema = null;
        ReferenceSchema referenceSchema = null;
        Map<String, Schema> properties = new LinkedHashMap<>();
        Map<String, Boolean> required = new HashMap<>();
        for (Schema subSchema : combinedSchema.getSubschemas()) {
            if (subSchema instanceof ConstSchema) {
                constSchema = (ConstSchema) subSchema;
            } else if (subSchema instanceof EnumSchema) {
                enumSchema = (EnumSchema) subSchema;
            } else if (subSchema instanceof NumberSchema) {
                numberSchema = (NumberSchema) subSchema;
            } else if (subSchema instanceof StringSchema) {
                stringSchema = (StringSchema) subSchema;
            } else if (subSchema instanceof CombinedSchema) {
                combinedSubschema = (CombinedSchema) subSchema;
            } else if (subSchema instanceof ReferenceSchema) {
                referenceSchema = (ReferenceSchema) subSchema;
            }
            collectPropertySchemas(subSchema, properties, required, new HashSet<>());
        }
        if (!properties.isEmpty()) {
            LinkedHashMap<String, ColumnDef> columnDefs = new LinkedHashMap<>();
            StructColumnDef structColumnDef = new StructColumnDef(columnDefs);
            ctx.put(combinedSchema, structColumnDef);
            for (Map.Entry<String, Schema> property : properties.entrySet()) {
                String subFieldName = property.getKey();
                Schema subSchema = property.getValue();
                ColumnDef columnDef = schemaToColumnDef(ctx, subSchema);
                if (!required.get(subFieldName)) {
                    columnDef.setColumnStrategy(NULL_STRATEGY);
                }
                columnDefs.put(subFieldName, columnDef);
            }
            return Tuple.of(combinedSchema, structColumnDef);
        } else if (combinedSubschema != null) {
            // Any combined subschema takes precedence over primitive subschemas
            return Tuple.of(combinedSubschema, schemaToColumnDef(ctx, combinedSubschema));
        } else if (constSchema != null) {
            if (stringSchema != null) {
                // Ignore the const, return the string
                return Tuple.of(stringSchema, schemaToColumnDef(ctx, stringSchema));
            } else if (numberSchema != null) {
                // Ignore the const, return the number or integer
                return Tuple.of(numberSchema, schemaToColumnDef(ctx, numberSchema));
            }
        } else if (enumSchema != null) {
            if (stringSchema != null) {
                // Return a string enum
                return Tuple.of(enumSchema, schemaToColumnDef(ctx, enumSchema));
            } else if (numberSchema != null) {
                // Ignore the enum, return the number or integer
                return Tuple.of(numberSchema, schemaToColumnDef(ctx, numberSchema));
            }
        } else if (stringSchema != null && stringSchema.getFormatValidator() != null) {
            if (numberSchema != null) {
                // This is a number or integer with a format
                return Tuple.of(numberSchema, schemaToColumnDef(ctx, numberSchema));
            }
            return Tuple.of(stringSchema, schemaToColumnDef(ctx, stringSchema));
        } else if (referenceSchema != null) {
            Schema referredSchema = referenceSchema.getReferredSchema();
            ColumnDef columnDef = ctx.get(referredSchema);
            if (columnDef != null) {
                return Tuple.of(referredSchema, columnDef);
            }
            return Tuple.of(referredSchema, schemaToColumnDef(ctx, referredSchema));
        }
        throw new IllegalArgumentException("Unsupported criterion "
            + combinedSchema.getCriterion() + " for " + combinedSchema);
    }

    private void collectPropertySchemas(
        Schema schema,
        Map<String, Schema> properties,
        Map<String, Boolean> required,
        Set<Schema> visited) {
        if (visited.contains(schema)) {
            return;
        } else {
            visited.add(schema);
        }
        if (schema instanceof CombinedSchema) {
            CombinedSchema combinedSchema = (CombinedSchema) schema;
            if (combinedSchema.getCriterion() == CombinedSchema.ALL_CRITERION) {
                for (Schema subSchema : combinedSchema.getSubschemas()) {
                    collectPropertySchemas(subSchema, properties, required, visited);
                }
            }
        } else if (schema instanceof ObjectSchema) {
            ObjectSchema objectSchema = (ObjectSchema) schema;
            for (Map.Entry<String, Schema> entry : objectSchema.getPropertySchemas().entrySet()) {
                String fieldName = entry.getKey();
                properties.put(fieldName, entry.getValue());
                required.put(fieldName, objectSchema.getRequiredProperties().contains(fieldName));
            }
        } else if (schema instanceof ReferenceSchema) {
            ReferenceSchema refSchema = (ReferenceSchema) schema;
            collectPropertySchemas(refSchema.getReferredSchema(), properties, required, visited);
        }
    }

    private Schema flattenSingletonUnion(CombinedSchema schema) {
        Collection<Schema> subschemas = schema.getSubschemas();
        int size = subschemas.size();
        if (size == 1) {
            return subschemas.iterator().next();
        } else if (size == 2) {
            boolean nullable = false;
            Schema notNullable = null;
            for (Schema subSchema : subschemas) {
                if (subSchema instanceof NullSchema) {
                    nullable = true;
                } else {
                    notNullable = subSchema;
                }
            }
            if (nullable && notNullable != null) {
                return notNullable;
            }
        }
        return null;
    }

    @Override
    public Object messageToColumn(
        Context ctx, ParsedSchema parsedSchema, Object message, ColumnDef columnDef) {
        Schema schema = (Schema) parsedSchema.rawSchema();
        return messageToColumn(ctx, schema, (JsonNode) message, columnDef);
    }

    private Object messageToColumn(
        Context ctx, Schema schema, JsonNode jsonNode, ColumnDef columnDef) {
        if (jsonNode == null) {
            return null;
        }
        if (schema instanceof BooleanSchema) {
            return jsonNode.asBoolean();
        } else if (schema instanceof NumberSchema) {
            NumberSchema numberSchema = (NumberSchema) schema;
            return numberSchema.requiresInteger()
                ? jsonNode.asLong()
                : jsonNode.asDouble();
        } else if (schema instanceof StringSchema) {
            return jsonNode.asText();
        } else if (schema instanceof ConstSchema) {
            return jsonNode.asText();
        } else if (schema instanceof EnumSchema) {
            return jsonNode.asText();
        } else if (schema instanceof CombinedSchema) {
            CombinedSchema combinedSchema = (CombinedSchema) schema;
            Schema singletonUnion = flattenSingletonUnion(combinedSchema);
            if (singletonUnion != null) {
                return messageToColumn(ctx, singletonUnion, jsonNode, columnDef);
            }
            if (combinedSchema.getCriterion() == CombinedSchema.ALL_CRITERION) {
                Schema subschema = allOfToConnectSchema(ctx, combinedSchema)._1;
                ColumnDef colDef = allOfToConnectSchema(ctx, combinedSchema)._2;
                return messageToColumn(ctx, subschema, jsonNode, colDef);
            }
            if (columnDef.getColumnType() == DuckDBColumnType.UNION) {
                UnionColumnDef unionColumnDef = (UnionColumnDef) columnDef;
                int unionIndex = 0;
                for (Schema subschema : combinedSchema.getSubschemas()) {
                    boolean valid = false;
                    try {
                        JsonSchema.validate(subschema, jsonNode);
                        valid = true;
                    } catch (Exception e) {
                        // noop
                    }
                    if (valid) {
                        String unionBranch = "u" + unionIndex;
                        ctx.putUnionBranch(unionColumnDef, unionBranch);
                        return messageToColumn(ctx, subschema, jsonNode,
                            unionColumnDef.getColumnDefs().get(unionBranch));
                    }
                    unionIndex++;
                }
            }
        } else if (schema instanceof ArraySchema) {
            ArraySchema arraySchema = (ArraySchema) schema;
            ArrayNode arrayNode = (ArrayNode) jsonNode;
            ListColumnDef listColumnDef = (ListColumnDef) columnDef;
            ColumnDef itemDef = listColumnDef.getItemDef();
            Object[] items = new Object[arrayNode.size()];
            for (int i = 0; i < arrayNode.size(); i++) {
                items[i] = messageToColumn(
                    ctx, arraySchema.getAllItemSchema(), arrayNode.get(i), itemDef);
            }
            return ctx.createArrayOf(itemDef.toDdl(), items);
        } else if (schema instanceof ObjectSchema) {
            ObjectSchema objectSchema = (ObjectSchema) schema;
            ObjectNode objectNode = (ObjectNode) jsonNode;
            if (columnDef.getColumnType() == DuckDBColumnType.MAP) {
                MapColumnDef mapColumnDef = (MapColumnDef) columnDef;
                Map<String, Object> map = new HashMap<>();
                Iterator<Entry<String, JsonNode>> entries = objectNode.fields();
                while (entries.hasNext()) {
                    Map.Entry<String, JsonNode> entry = entries.next();
                    String name = entry.getKey();
                    Object newValue = messageToColumn(
                        ctx,
                        objectSchema.getSchemaOfAdditionalProperties(),
                        entry.getValue(),
                        mapColumnDef.getValueDef()
                    );
                    map.put(name, newValue);
                }
                return ctx.createMap(mapColumnDef.toDdl(), map);
            }
            StructColumnDef structColumnDef = (StructColumnDef) columnDef;
            Map<String, Schema> properties = objectSchema.getPropertySchemas();
            Object[] attributes = new Object[properties.size()];
            int i = 0;
            for (Map.Entry<String, Schema> entry : properties.entrySet()) {
                String name = entry.getKey();
                ColumnDef fieldColumnDef = structColumnDef.getColumnDefs().get(name);
                Object newValue = messageToColumn(
                    ctx, entry.getValue(), objectNode.get(name), fieldColumnDef);
                attributes[i++] = newValue;
            }
            return ctx.createStruct(structColumnDef.toDdl(), attributes);
        } else if (schema instanceof ReferenceSchema) {
            ReferenceSchema referenceSchema = (ReferenceSchema) schema;
            return messageToColumn(ctx, referenceSchema.getReferredSchema(), jsonNode, columnDef);
        }
        throw new IllegalArgumentException();
    }
}
