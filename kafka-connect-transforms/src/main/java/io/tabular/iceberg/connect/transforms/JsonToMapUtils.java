package io.tabular.iceberg.connect.transforms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class JsonToMapUtils {
    private static final Map<Class<? extends JsonNode>, Schema> JSON_NODE_TO_SCHEMA = getJsonNodeToSchema();
    private static Map<Class<? extends JsonNode>, Schema> getJsonNodeToSchema() {
        final Map<Class<? extends JsonNode>, Schema> map = Maps.newHashMap();
        map.put(BinaryNode.class, Schema.OPTIONAL_BYTES_SCHEMA);
        map.put(BooleanNode.class, Schema.OPTIONAL_BOOLEAN_SCHEMA);
        map.put(TextNode.class, Schema.OPTIONAL_STRING_SCHEMA);
        map.put(IntNode.class, Schema.OPTIONAL_INT32_SCHEMA);
        map.put(LongNode.class, Schema.OPTIONAL_INT64_SCHEMA);
        map.put(FloatNode.class, Schema.OPTIONAL_FLOAT32_SCHEMA);
        map.put(DoubleNode.class, Schema.OPTIONAL_FLOAT64_SCHEMA);
        map.put(ArrayNode.class, Schema.OPTIONAL_STRING_SCHEMA);
        map.put(ObjectNode.class, Schema.OPTIONAL_STRING_SCHEMA);
        map.put(BigIntegerNode.class, Schema.OPTIONAL_STRING_SCHEMA);
        map.put(DecimalNode.class, Schema.OPTIONAL_STRING_SCHEMA);
        return ImmutableMap.copyOf(map);
    }
    public static void addToSchema(String fieldName, Schema schema, SchemaBuilder builder) {
        if (schema != null) {
            builder.field(fieldName, schema);
        }
    }

    public static SchemaBuilder addFieldSchemaBuilder(Map.Entry<String, JsonNode> kv, SchemaBuilder builder) {
        String key = kv.getKey();
        JsonNode value = kv.getValue();
        addToSchema(key, schemaFromNode(value), builder);
        return builder;
    }

    public static Schema schemaFromNode(JsonNode node) {
        if (!node.isNull() && !node.isMissingNode()) {
            if (node.isArray()) {
                ArrayNode array = (ArrayNode) node;
                // can't create a schema for an empty array since the inner type is not known.
                if (!array.isEmpty()) {
                    Class<? extends JsonNode> arrayType = arrayNodeType(array);
                    if (arrayType != null) {
                        if (arrayType != NullNode.class && arrayType != MissingNode.class) {
                            if (arrayType == ObjectNode.class) {
                                // arrays of objects become strings
                                int objectCount = 0;
                                int emptyObjectCount = 0;
                                for (Iterator<JsonNode> it = array.elements(); it.hasNext(); ) {
                                    JsonNode element = it.next();
                                    objectCount += 1;
                                    if (!element.elements().hasNext()) {
                                       emptyObjectCount += 1;
                                    }
                                }
                                if (objectCount == emptyObjectCount) {
                                    return null;
                                } else {
                                    return SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build();
                                }
                            } else if (arrayType == ArrayNode.class) {
                                // nested array case
                                // need to protect against arrays of empty arrays, arrays of empty objects, arrays inconsistent types, etc.
                                List<Schema> nestedSchemas = Lists.newArrayList();
                                boolean[] hasValidSchema = {true};
                                node.elements().forEachRemaining(nodeElement -> {
                                    Schema nestedElementSchema = schemaFromNode(nodeElement);
                                    if (nestedElementSchema == null) {
                                        hasValidSchema[0] = false;
                                    }
                                    nestedSchemas.add(nestedElementSchema);
                                });
                                if (!nestedSchemas.isEmpty() && hasValidSchema[0]) {
                                    boolean allMatch = nestedSchemas.stream().allMatch(schema -> schema.equals(nestedSchemas.get(0)));
                                    if (allMatch) {
                                        return SchemaBuilder.array(nestedSchemas.get(0)).optional().build();
                                    } else {
                                        return SchemaBuilder.array(SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build()).optional().build();
                                    }
                                } else {
                                    return null;
                                }
                            } else {

                                // if we are a consistent primitive
                                return SchemaBuilder.array(JSON_NODE_TO_SCHEMA.get(arrayType)).optional().build();
                            }
                        }
                    } else {
                        // if types of the array are inconsistent, convert to a string
                        return SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build();
                    }
                }
            } else if (node.isObject()) {
                if (node.elements().hasNext()) {
                    return JSON_NODE_TO_SCHEMA.get(node.getClass());
                }
            } else {
                return JSON_NODE_TO_SCHEMA.get(node.getClass());
            }
        }
        return null;
    }

    /* Kafka Connect arrays must all be the same type */
    public static Class<? extends JsonNode> arrayNodeType(ArrayNode array) {
        final List<Class<? extends JsonNode>> arrayType = Lists.newArrayList();
        arrayType.add(null);
        final boolean[] allTypesConsistent = {true};
        // breaks on number.
        array.elements().forEachRemaining(node -> {
            Class<? extends JsonNode> type = node.getClass();
            if (arrayType.get(0) == null) {
                arrayType.set(0,type);
            }
            if (type != arrayType.get(0)) {
                allTypesConsistent[0] = false;
            }
        });

        if (!allTypesConsistent[0]) {
            return null;
        }

        return arrayType.get(0);
    }
    public static Struct addToStruct(ObjectNode node, Schema schema, Struct struct) {
        schema.fields().forEach(field -> {
            JsonNode element = node.get(field.name());
            Schema.Type targetType = field.schema().type();
            if (targetType == Schema.Type.ARRAY) {
                struct.put(field.name(), populateArray(element, field.schema().valueSchema(), field.name(), Lists.newArrayList()));
            } else {
                struct.put(field.name(), extractSimpleValue(element, targetType, field.name()));
            }
        });
        return struct;
    }

    public static Object extractSimpleValue(JsonNode node, Schema.Type type, String fieldName) {
        Object obj;
        switch (type) {
            case STRING:
                if (node.isTextual()) {
                    obj = node.textValue();
                } else {
                    obj = node.toString();
                }
                break;
            case BOOLEAN:
                obj = node.booleanValue();
                break;
            case INT32:
                obj = node.intValue();
                break;
            case INT64:
                obj = node.longValue();
                break;
            case FLOAT32:
                obj = node.floatValue();
                break;
            case FLOAT64:
                obj = node.doubleValue();
                break;
            case BYTES:
                try {
                    obj = node.binaryValue();
                } catch (Exception e) {
                    throw new RuntimeException(String.format("parsing binary value threw exception for %s", fieldName), e);
                }
                break;
            default:
                throw new RuntimeException(String.format("Unexpected type %s for field %s", type, fieldName));
        }
        return obj;
    }

    private static List<Object> populateArray(JsonNode node, Schema schema, String fieldName, List<Object> acc) {
        if (schema.type() == Schema.Type.ARRAY) {
            for (Iterator<JsonNode> it = node.elements(); it.hasNext(); ) {
                JsonNode arrayNode = it.next();
                List<Object> nestedList = Lists.newArrayList();
                acc.add(populateArray(arrayNode, schema.valueSchema(), fieldName, nestedList));
            }
        } else {
            node.elements().forEachRemaining(arrayEntry -> acc.add(extractSimpleValue(arrayEntry, schema.type(), fieldName)));
        }
        return acc;
    }
}
