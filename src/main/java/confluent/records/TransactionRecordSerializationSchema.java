package confluent.records;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class TransactionRecordSerializationSchema
        implements KafkaRecordSerializationSchema<TransactionRecord> {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper objectMapper =
            JsonMapper.builder()
                    .build()
                    .registerModule(new JavaTimeModule())
                    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    private String topic;

    public TransactionRecordSerializationSchema() {
    }

    public TransactionRecordSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            TransactionRecord element, KafkaSinkContext kafkaSinkContext, Long aLong) {


        try {
            return new ProducerRecord<>(
                    topic,
                    null,
                    element.time.toEpochMilli(),
                    null,
                    objectMapper.writeValueAsBytes(element));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not serialize record: " + element, e);
        }
    }
}