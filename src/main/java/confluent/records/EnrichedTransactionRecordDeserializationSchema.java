package confluent.records;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class EnrichedTransactionRecordDeserializationSchema
        extends AbstractDeserializationSchema<EnrichedTransactionRecord> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) throws Exception {
        super.open(context);
        objectMapper = JsonMapper.builder().build().registerModule(new JavaTimeModule());
    }

    @Override
    public EnrichedTransactionRecord deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, EnrichedTransactionRecord.class);
    }
}
