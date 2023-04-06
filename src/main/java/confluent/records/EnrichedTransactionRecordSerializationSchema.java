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

public class EnrichedTransactionRecordSerializationSchema
        implements KafkaRecordSerializationSchema<EnrichedTransactionRecord> {

    private static final long serialVersionUID = 1L;

    private String topic;
    private static final ObjectMapper objectMapper =
            JsonMapper.builder()
                    .build()
                    .registerModule(new JavaTimeModule())
                    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    public EnrichedTransactionRecordSerializationSchema() {
    }

    public EnrichedTransactionRecordSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            EnrichedTransactionRecord element, KafkaSinkContext kafkaSinkContext, Long aLong) {


        try {
            return new ProducerRecord<>(
                    topic,
                    null,
                    element.ts.toEpochMilli(),
                    null,
                    objectMapper.writeValueAsBytes(element));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not serialize record: " + element, e);
        }
    }
}
