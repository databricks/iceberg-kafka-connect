package io.tabular.iceberg.connect.transforms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.*;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

public class JsonToMapTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String JSON_LEVEL = "transforms.json.level";

    private static final ObjectReader mapper = new ObjectMapper().reader();

    private int JSON_LEVEL_VALUE = 1;

    public static final ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            JSON_LEVEL,
                            ConfigDef.Type.INT,
                            1,
                            ConfigDef.Importance.MEDIUM,
                            "Positive value after which level in the json to convert to Map<String, String>");


    private static final String ALL_JSON_SCHEMA_FIELD = "payload";
    private static final Schema ALL_JSON_SCHEMA = SchemaBuilder.struct().field(ALL_JSON_SCHEMA_FIELD, Schema.STRING_SCHEMA).build();


    @Override
    public R apply(R record) {
        if (record.value() == null) {
            return record;
        } else {
            return process(record);
        }
    }

    private R process(R record) {
        if (!(record.value() instanceof String)) {
            // filter out non-string messages
            return newNullRecord(record);
        }

        String json = (String) record.value();
        JsonNode obj;

        try {
            obj = mapper.readTree(json);
        } catch (Exception e) {
            throw new RuntimeException(String.format("record.value is not valid json for record.value: %s", collectRecordDetails(record)), e);
        }

        if (!(obj instanceof ObjectNode)) {
            throw new RuntimeException(String.format("Expected json object for record.value: %s", collectRecordDetails(record)));
        }

        if (JSON_LEVEL_VALUE == 0) {
            // return the json as a single field, after validating it's actually json
            return singleField(record);
        }

        return structRecord(record, (ObjectNode) obj);
    }

    private R singleField(R record) {
        Struct struct = new Struct(ALL_JSON_SCHEMA).put(ALL_JSON_SCHEMA_FIELD, record.value());
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), ALL_JSON_SCHEMA, struct, record.timestamp(), record.headers());
    }

    private R structRecord(R record, ObjectNode contents) {
        SchemaBuilder builder = SchemaBuilder.struct();
        contents.fields().forEachRemaining(entry -> JsonToMapUtils.addFieldSchemaBuilder(entry, builder));
        Schema schema = builder.build();
        Struct value = JsonToMapUtils.addToStruct(contents, schema, new Struct(schema));
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), schema, value, record.timestamp(), record.headers());
    }

    private String collectRecordDetails(R record) {
        if (record instanceof SinkRecord) {
            SinkRecord sinkRecord = (SinkRecord) record;
            return            String.format("topic %s partition %s offset %s", sinkRecord.topic(), sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset());
        } else {
            return String.format("topic %s partition %S", record.topic(), record.kafkaPartition());
        }
    }



    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    private R newNullRecord(R record) {
        return record.newRecord(record.topic(), record.kafkaPartition(), null, null, null, null, record.timestamp(), record.headers());
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        JSON_LEVEL_VALUE = config.getInt(JSON_LEVEL);
        if (JSON_LEVEL_VALUE < 0){
            throw new ConfigException(String.format("%s must be >= 0",JSON_LEVEL));
        }

    }
}
