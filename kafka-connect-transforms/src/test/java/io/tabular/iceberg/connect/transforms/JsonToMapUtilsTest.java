package io.tabular.iceberg.connect.transforms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class JsonToMapUtilsTest extends FileLoads {
    private final ObjectMapper mapper = new ObjectMapper();

    private final ObjectNode objNode = loadJson(mapper);

    private ObjectNode loadJson(ObjectMapper mapper) {
        try {
            return (ObjectNode) mapper.readTree(getFile("jsonmap.json"));
        } catch (Exception e) {
            throw new RuntimeException("failed to load jsonmap.json in test", e);
        }
    }

    @Test
    @DisplayName("addToSchema should add schemas to builder unless schema is null")
    public void addToSchema() {
        SchemaBuilder builder = SchemaBuilder.struct();
        JsonToMapUtils.addToSchema("good", Schema.STRING_SCHEMA, builder);
        JsonToMapUtils.addToSchema("null", null, builder);
        Schema result = builder.build();
        assertThat(result.fields()).isEqualTo(Lists.newArrayList(new Field("good", 0, Schema.STRING_SCHEMA)));
    }

    @Test
    @DisplayName("extractSimpleValue extracts type from Node based on Schema")
    public void primitiveBasedOnSchemaHappyPath() {
        JsonNode stringNode = objNode.get("string");
        JsonNode booleanNode = objNode.get("boolean");
        JsonNode intNode = objNode.get("int");
        JsonNode longNode = objNode.get("long");
        JsonNode floatIsDoubleNode = objNode.get("float_is_double");
        JsonNode doubleNode = objNode.get("double");
        JsonNode bytesNode = objNode.get("bytes");

        assertThat(JsonToMapUtils.extractSimpleValue(stringNode, Schema.Type.STRING, "")).isEqualTo("string");
        assertThat(JsonToMapUtils.extractSimpleValue(booleanNode, Schema.Type.BOOLEAN, "")).isEqualTo(true);
        assertThat(JsonToMapUtils.extractSimpleValue(intNode, Schema.Type.INT32, "")).isEqualTo(42);
        assertThat(JsonToMapUtils.extractSimpleValue(longNode, Schema.Type.INT64,"")).isEqualTo(3147483647L);
        assertThat(JsonToMapUtils.extractSimpleValue(floatIsDoubleNode, Schema.Type.FLOAT64, "")).isEqualTo(3.0);
        assertThat(JsonToMapUtils.extractSimpleValue(doubleNode, Schema.Type.FLOAT64, "")).isEqualTo( 0.3);
        byte[] byteResult = (byte[]) JsonToMapUtils.extractSimpleValue(bytesNode, Schema.Type.BYTES, "");
        assertArrayEquals(byteResult, Base64.getDecoder().decode("SGVsbG8="));
    }

    @Test
    @DisplayName("extractSimpleValue converts complex nodes to strings if schema is string")
    public void exactStringsFromComplexNodes() {
        JsonNode arrayObjects = objNode.get("array_objects");
        assertInstanceOf(ArrayNode.class, arrayObjects);

        JsonNode nestedObjNode = objNode.get("nested_obj");
        assertInstanceOf(ObjectNode.class, nestedObjNode);

        JsonNode arrayDifferentTypes = objNode.get("array_different_types");
        assertInstanceOf(ArrayNode.class, arrayDifferentTypes);

        JsonNode bigInt = objNode.get("bigInt");
        assertInstanceOf(BigIntegerNode.class, bigInt);

        assertThat(JsonToMapUtils.extractSimpleValue(arrayObjects, Schema.Type.STRING, "")).isEqualTo("[{\"key\":1}]");
        assertThat(JsonToMapUtils.extractSimpleValue(nestedObjNode, Schema.Type.STRING, "")).isEqualTo("{\"one\":1}");
        assertThat(JsonToMapUtils.extractSimpleValue(arrayDifferentTypes, Schema.Type.STRING, "")).isEqualTo("[\"one\",1]");
        assertThat(JsonToMapUtils.extractSimpleValue(bigInt, Schema.Type.STRING, "")).isEqualTo("354736184430273859332531123456");
    }

    @Test
    @DisplayName("extractSimpleValue throws for non-primitive schema types")
    public void primitiveBasedOnSchemaThrows() {
        assertThrows(RuntimeException.class, () -> JsonToMapUtils.extractSimpleValue(objNode.get("string"), Schema.Type.STRUCT, ""));
    }

    @Test
    @DisplayName("arrayNodeType returns a type if all elements of the array are the same type")
    public void determineArrayNodeType() {
        ArrayNode arrayInt = (ArrayNode) objNode.get("array_int");
        ArrayNode arrayArrayInt = (ArrayNode) objNode.get("array_array_int");
        assertThat(IntNode.class).isEqualTo(JsonToMapUtils.arrayNodeType(arrayInt));
        assertThat(ArrayNode.class).isEqualTo(JsonToMapUtils.arrayNodeType(arrayArrayInt));
    }

    @Test
    @DisplayName("arrayNodeType returns null if elements of the array are different types")
    public void determineArrayNodeTypeNotSameTypes() {
        ArrayNode mixedArray = (ArrayNode) objNode.get("array_different_types");
        assertThat(JsonToMapUtils.arrayNodeType(mixedArray)).isNull();
    }

    @Test
    @DisplayName("schemaFromNode returns null for NullNode and MissingNode types")
    public void schemaFromNodeNullOnNullNodes() {
        JsonNode nullNode = NullNode.getInstance();
        JsonNode missingNode = MissingNode.getInstance();
        assertThat(JsonToMapUtils.schemaFromNode(nullNode)).isNull();
        assertThat(JsonToMapUtils.schemaFromNode(missingNode)).isNull();
    }

    @Test
    @DisplayName("schemaFromNode returns String schema for ObjectNodes")
    public void schemaFromNodeStringsFromObjectNodes() {
        assertThat(JsonToMapUtils.schemaFromNode(objNode)).isEqualTo(Schema.OPTIONAL_STRING_SCHEMA);
    }

    @Test
    @DisplayName("schemaFromNode returns null for empty ObjectNodes")
    public void schemaFromNodeNullEmptyObjectNodes() {
        JsonNode node = objNode.get("empty_obj");
        assertInstanceOf(ObjectNode.class, node);
        assertThat(JsonToMapUtils.schemaFromNode(node)).isNull();
    }

    @Test
    @DisplayName("schemaFromNode returns String schema for BigInteger nodes")
    public void schemaFromNodeStringForBigInteger() {
        JsonNode node = objNode.get("bigInt");
        assertInstanceOf(BigIntegerNode.class, node);
        assertThat(JsonToMapUtils.schemaFromNode(node)).isEqualTo(Schema.OPTIONAL_STRING_SCHEMA);
    }

    @Test
    @DisplayName("schemaFromNode returns primitive Schemas for primitive nodes")
    public void schemaFromNodePrimitiveSchemasFromPrimitiveNodes() {
        JsonNode intNode = objNode.get("int");
        JsonNode doubleNode = objNode.get("double");
        assertInstanceOf(IntNode.class, intNode);
        assertInstanceOf(DoubleNode.class, doubleNode);
        assertThat(JsonToMapUtils.schemaFromNode(intNode)).isEqualTo(Schema.OPTIONAL_INT32_SCHEMA);
        assertThat(JsonToMapUtils.schemaFromNode(doubleNode)).isEqualTo(Schema.OPTIONAL_FLOAT64_SCHEMA);
    }

    @Test
    @DisplayName("schemaFromNode returns Array String schema for ArrayNodes with ObjectNode elements")
    public void schemaFromNodeArrayStringFromArrayObjects() {
        JsonNode arrayObjects = objNode.get("array_objects");
        assertInstanceOf(ArrayNode.class, arrayObjects);
        assertThat(JsonToMapUtils.schemaFromNode(arrayObjects)).isEqualTo(SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build());
    }

    @Test
    @DisplayName("schemaFromNode returns Array String schema for ArrayNodes with inconsistent types")
    public void schemaFromNodeArrayStringFromInconsistentArrayNodes() {
        JsonNode inconsistent = objNode.get("array_different_types");
        assertInstanceOf(ArrayNode.class, inconsistent);
        assertThat(JsonToMapUtils.schemaFromNode(inconsistent)).isEqualTo(SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build());
    }

    @Test
    @DisplayName("schemaFromNode returns Array[Array[String]] for ArrayNodes of ArrayNodes with inconsistent types")
    public void schemaFromNodeArraysArrays() {
        JsonNode node = objNode.get("array_array_inconsistent");
        assertInstanceOf(ArrayNode.class, node);

        Schema expected = SchemaBuilder.array(SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build()).optional().build();
        Schema result = JsonToMapUtils.schemaFromNode(node);
        assertThat(result).isEqualTo(expected);
    }
    @Test
    @DisplayName("schemaFromNode returns Array[Array[String]] for ArrayNodes of ArrayNodes of objects")
    public void schemaFromNodeArrayArrayObjects() {
        JsonNode node = objNode.get("array_array_objects");
        assertInstanceOf(ArrayNode.class, node);
        Schema expected = SchemaBuilder.array(SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build()).optional().build();
        Schema result = JsonToMapUtils.schemaFromNode(node);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    @DisplayName("schemaFromNode returns Array[Array[Int]] for ArrayNodes of ArrayNodes of IntNode")
    public void schemaFromNodeArrayArrayOfArrayArrayInt() {
        JsonNode node = objNode.get("array_array_int");
        assertInstanceOf(ArrayNode.class, node);
        Schema expected = SchemaBuilder.array(SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build()).optional().build();
        Schema result = JsonToMapUtils.schemaFromNode(node);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    @DisplayName("schemaFromNode returns null for empty ArrayNodes")
    public void schemaFromNodeNullFromEmptyArray() {
        JsonNode node = objNode.get("empty_arr");
        assertInstanceOf(ArrayNode.class, node);
        assertThat(JsonToMapUtils.schemaFromNode(node)).isNull();
    }
    @Test
    @DisplayName("schemaFromNode returns null for empty Array of Array nodes")
    public void schemaFromNodeEmptyArrayOfEmptyArrays() {
        JsonNode node = objNode.get("empty_arr_arr");
        assertInstanceOf(ArrayNode.class, node);
        assertThat(JsonToMapUtils.schemaFromNode(node)).isNull();
    }

    @Test
    @DisplayName("schemaFromNode returns null for empty array of empty object")
    public void schemaFromNodeNullArrayEmptyObject() {
        JsonNode node = objNode.get("array_empty_object");
        assertInstanceOf(ArrayNode.class, node);
        assertThat(JsonToMapUtils.schemaFromNode(node)).isNull();
    }
    @Test
    @DisplayName("schemaFromNode returns Array[String] for array of objects even if one object is empty")
    public void schemaFromNodeMixedObjectsOneEmpty() {
        JsonNode node = objNode.get("nested_object_contains_empty");
        assertInstanceOf(ArrayNode.class, node);
        Schema expected = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build();
        Schema result = JsonToMapUtils.schemaFromNode(node);
        assertThat(result).isEqualTo(expected);
    }

   @Test
   public void addToStruct() {
       SchemaBuilder builder = SchemaBuilder.struct();
       objNode.fields().forEachRemaining(entry -> JsonToMapUtils.addFieldSchemaBuilder(entry, builder));
       Schema schema = builder.build();

       Struct result = new Struct(schema);
       JsonToMapUtils.addToStruct(objNode, schema, result);

       assertThat(result.get("string")).isEqualTo("string");
       assertThat(result.get("int")).isEqualTo(42);
       assertThat(result.get("long")).isEqualTo(3147483647L);
       assertThat(result.get("boolean")).isEqualTo(true);
       assertThat(result.get("float_is_double")).isEqualTo(3.0);
       assertThat(result.get("double")).isEqualTo(0.3);
       // we don't actually convert to bytes when parsing the json
       assertThat(result.get("bytes")).isEqualTo("SGVsbG8=");
       // no way to represent BigInteger in a SinkRecord w/o custom annotations
       assertThat(result.get("bigInt")).isEqualTo("354736184430273859332531123456");
       assertThat(result.get("nested_object_contains_empty")).isEqualTo(Lists.newArrayList("{}", "{\"one\":1}"));
       assertThat(result.get("array_int")).isEqualTo(Lists.newArrayList(1, 1));
       assertThat(result.get("array_array_int")).isEqualTo(Lists.newArrayList(Lists.newArrayList(1,1), Lists.newArrayList(2,2)));
       assertThat(result.get("array_objects")).isEqualTo(Lists.newArrayList("{\"key\":1}"));
       assertThat(result.get("array_array_objects")).isEqualTo(Lists.newArrayList(Lists.newArrayList("{\"key\":1}"), Lists.newArrayList("{\"key\":2}")));
       assertThat(result.get("array_array_inconsistent")).isEqualTo(Lists.newArrayList(Lists.newArrayList("1"), Lists.newArrayList("2.0")));
       assertThat(result.get("array_different_types")).isEqualTo(Lists.newArrayList("one", "1"));
       assertThat(result.get("nested_obj")).isEqualTo("{\"one\":1}");

       // assert empty fields don't show up on the struct
       assertThrows(RuntimeException.class, () -> result.get("null"));
       assertThrows(RuntimeException.class, () -> result.get("empty_obj"));
       assertThrows(RuntimeException.class, () -> result.get("empty_arr"));
       assertThrows(RuntimeException.class, () -> result.get("empty_arr_arr"));
       assertThrows(RuntimeException.class, () -> result.get("array_empty_object"));
   }

}





