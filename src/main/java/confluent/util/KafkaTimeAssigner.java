package confluent.util;

import confluent.records.TransactionRecord;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class KafkaTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor<TransactionRecord> {

    /**
     * Returns message timestamp.
     */
    public KafkaTimeAssigner() {
        super(Time.seconds(5));
    }

    @Override
    public long extractTimestamp(TransactionRecord r) {
        return r.time.toEpochMilli();
    }
}
