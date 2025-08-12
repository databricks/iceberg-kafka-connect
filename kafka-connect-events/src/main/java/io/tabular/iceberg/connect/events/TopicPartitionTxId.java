package io.tabular.iceberg.connect.events;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.util.Utf8;

public class TopicPartitionTxId implements Element {

    private String topic;
    private Integer partition;
    private Long txId;
    private final Schema avroSchema;

    public static final Schema AVRO_SCHEMA =
            SchemaBuilder.builder()
                    .record(TopicPartitionTxId.class.getName())
                    .fields()
                    .name("topic")
                    .prop(FIELD_ID_PROP, DUMMY_FIELD_ID)
                    .type()
                    .stringType()
                    .noDefault()
                    .name("partition")
                    .prop(FIELD_ID_PROP, DUMMY_FIELD_ID)
                    .type()
                    .intType()
                    .noDefault()
                    .name("tx_id")
                    .prop(FIELD_ID_PROP, DUMMY_FIELD_ID)
                    .type()
                    .nullable()
                    .longType()
                    .noDefault()
                    .endRecord();

    // Used by Avro reflection to instantiate this class when reading events
    public TopicPartitionTxId(Schema avroSchema) {
        this.avroSchema = avroSchema;
    }

    public TopicPartitionTxId(String topic, int partition, Long txId) {
        this.topic = topic;
        this.partition = partition;
        this.txId = txId;
        this.avroSchema = AVRO_SCHEMA;
    }

    public String topic() {
        return topic;
    }

    public Integer partition() {
        return partition;
    }

    public Long txId() {
        return txId;
    }

    @Override
    public Schema getSchema() {
        return avroSchema;
    }

    @Override
    public void put(int i, Object v) {
        switch (i) {
            case 0:
                this.topic = v instanceof Utf8 ? v.toString() : (String) v;
                return;
            case 1:
                this.partition = (Integer) v;
                return;
            case 2:
                this.txId = (Long) v;
                return;
            default:
                // ignore the object, it must be from a newer version of the format
        }
    }

    @Override
    public Object get(int i) {
        switch (i) {
            case 0:
                return topic;
            case 1:
                return partition;
            case 2:
                return txId;
            default:
                throw new UnsupportedOperationException("Unknown field ordinal: " + i);
        }
    }
}