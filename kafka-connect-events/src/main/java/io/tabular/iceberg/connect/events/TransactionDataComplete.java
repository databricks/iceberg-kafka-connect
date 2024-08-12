package io.tabular.iceberg.connect.events;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.iceberg.connect.events.PayloadType.DATA_COMPLETE;

public class TransactionDataComplete implements org.apache.iceberg.connect.events.Payload {

    private UUID commitId;
    private List<TopicPartitionOffset> assignments;
    private List<TopicPartitionTransaction> txIds;
    private final Schema avroSchema;

    static final int COMMIT_ID = 10_100;
    static final int ASSIGNMENTS = 10_101;
    static final int ASSIGNMENTS_ELEMENT = 10_102;
    static final int TX_IDS = 10_201;
    static final int TX_IDS_ELEMENT = 10_202;

    private static final Types.StructType ICEBERG_SCHEMA =
            Types.StructType.of(
                    Types.NestedField.required(COMMIT_ID, "commit_id", Types.UUIDType.get()),
                    Types.NestedField.optional(
                            ASSIGNMENTS,
                            "assignments",
                            Types.ListType.ofRequired(ASSIGNMENTS_ELEMENT, TopicPartitionOffset.ICEBERG_SCHEMA)),
                    Types.NestedField.optional(
                            TX_IDS,
                            "tx_ids",
                            Types.ListType.ofRequired(TX_IDS_ELEMENT, TopicPartitionTransaction.ICEBERG_SCHEMA)));

    private static final Schema AVRO_SCHEMA = AvroSchemaUtil.convert(ICEBERG_SCHEMA,
            TransactionDataComplete.class.getName());

    public TransactionDataComplete(Schema avroSchema) {
        this.avroSchema = avroSchema;
    }

    public TransactionDataComplete(UUID commitId, List<TopicPartitionOffset> assignments, List<TopicPartitionTransaction> txIds) {
        this.commitId = commitId;
        this.assignments = assignments;
        this.txIds = txIds;
        this.avroSchema = AVRO_SCHEMA;
    }

    public UUID commitId() {
        return commitId;
    }

    public List<TopicPartitionOffset> assignments() {
        return assignments;
    }

    public List<TopicPartitionTransaction> txIds() {
        return txIds;
    }

    @Override
    public Schema getSchema() {
        return avroSchema;
    }

    @Override
    public PayloadType type() {
        return DATA_COMPLETE;
    }

    @Override
    public Types.StructType writeSchema() {
        return ICEBERG_SCHEMA;
    }


    @Override
    @SuppressWarnings("unchecked")
    public void put(int i, Object v) {
        switch (positionToId(i, avroSchema)) {
            case COMMIT_ID:
                this.commitId = (UUID) v;
                return;
            case ASSIGNMENTS:
                this.assignments = (List<TopicPartitionOffset>) v;
                return;
            case TX_IDS:
                if (v instanceof List) {
                    List<GenericData.Record> records = (List<GenericData.Record>) v;
                    this.txIds = records.stream()
                            .map(TransactionDataComplete::toTopicPartitionTransaction)
                            .collect(Collectors.toList());
                }
                return;
            default:
                // ignore the object, it must be from a newer version of the format
        }
    }

    @Override
    public Object get(int i) {
        switch (positionToId(i, avroSchema)) {
            case COMMIT_ID:
                return commitId;
            case ASSIGNMENTS:
                return assignments;
            case TX_IDS:
                return txIds;
            default:
                throw new UnsupportedOperationException("Unknown field ordinal: " + i);
        }
    }

    static int positionToId(int position, Schema avroSchema) {
        List<Schema.Field> fields = avroSchema.getFields();
        Preconditions.checkArgument(
                position >= 0 && position < fields.size(), "Invalid field position: " + position);
        Object val = fields.get(position).getObjectProp(AvroSchemaUtil.FIELD_ID_PROP);
        return val == null ? -1 : (int) val;
    }

    public static TopicPartitionTransaction toTopicPartitionTransaction(GenericData.Record record) {
        Long txId = (long) record.get("txId");
        String topic = record.get("topic").toString();
        int partition = (int) record.get("partition");
        return new TopicPartitionTransaction(topic, partition, txId);
    }
}
