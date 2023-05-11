/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package confluent;

import confluent.records.EnrichedTransactionRecord;
import confluent.records.EnrichedTransactionRecordDeserializationSchema;
import confluent.records.LoyaltyLevel;
import confluent.records.LoyaltyLevelSerializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

/**
 * Skeleton for a Flink DataStream Job.
 */
public class SumFilterJob {

    public static void main(String[] args) throws Exception {

        /******************************************************************************************
         * Getting the environment
         ******************************************************************************************/
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        /******************************************************************************************
         * Creating Kafka source
         ******************************************************************************************/
        String topic = "enrichedTransactions";
        String broker = "localhost:9092";

        KafkaSource<EnrichedTransactionRecord> source =
                KafkaSource.<EnrichedTransactionRecord>builder()
                        .setBootstrapServers(broker)
                        .setTopics(topic)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new EnrichedTransactionRecordDeserializationSchema())
                        .build();


        /******************************************************************************************
         * Creating Kafka sink
         ******************************************************************************************/

        Properties properties = new Properties();
        properties.setProperty("transaction.timeout.ms", "7200");

        String topic3 = "loyaltyLevels";

        KafkaSink<LoyaltyLevel> sink =
                KafkaSink.<LoyaltyLevel>builder()
                        .setBootstrapServers(broker)
                        .setKafkaProducerConfig(properties)
                        .setRecordSerializer(new LoyaltyLevelSerializationSchema(topic3))
                        .setTransactionalIdPrefix("jans4")
                        .build();

        /******************************************************************************************
         * Reading from Kafka data source
         ******************************************************************************************/

        DataStream<EnrichedTransactionRecord> records =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        /******************************************************************************************
         * Processing begins...
         ******************************************************************************************/

        // Create Keyed Stream
        KeyedStream<EnrichedTransactionRecord, String> keyRecords =
                records.keyBy(kr -> kr.userId);

        // Sum total amounts per user
        DataStream<EnrichedTransactionRecord> sumAmount = keyRecords
                .sum("totalPrice");

        // Filter big spenders
        DataStream<EnrichedTransactionRecord> filteredSums =
                sumAmount.filter(r -> r.totalPrice > 1000);

        // Assign reward levels
        DataStream<LoyaltyLevel> loyaltyLevels =
                filteredSums.map(ll -> {
                    if (ll.totalPrice > 10000) {
                        return
                                new LoyaltyLevel(ll.userId, ll.totalPrice, "GOLD");
                    } else if (ll.totalPrice > 5000) {
                        return
                                new LoyaltyLevel(ll.userId, ll.totalPrice, "SILVER");
                    } else return
                            new LoyaltyLevel(ll.userId, ll.totalPrice, "BRONZE");
                });

        loyaltyLevels.sinkTo(sink);


        /******************************************************************************************
         * Executing and printing the results
         ******************************************************************************************/


        // Execute program, beginning computation.
        env.execute("Flink Java API Skeleton");
    }


}
