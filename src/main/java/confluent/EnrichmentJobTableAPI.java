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

import confluent.records.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.apache.flink.table.api.Expressions.$;

/**
 * Skeleton for a Flink DataStream Job.
 */
public class EnrichmentJobTableAPI {

    public static void main(String[] args) throws Exception {

        /******************************************************************************************
         * Getting the environment
         ******************************************************************************************/
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        /******************************************************************************************
         * Creating Kafka source
         ******************************************************************************************/
        String topic = "transactions";
        String broker = "localhost:9092";

        KafkaSource<TransactionRecord> source =
                KafkaSource.<TransactionRecord>builder()
                        .setBootstrapServers(broker)
                        .setTopics(topic)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new TransactionRecordDeserializationSchema())
                        .build();

        /******************************************************************************************
         * Creating File CSV sources
         ******************************************************************************************/

        Path inputFolder = new Path("data/users");
        Path inputFolder2 = new Path("data/products");

        CsvReaderFormat<User> csvFormatUser = CsvReaderFormat.forPojo(User.class);
        CsvReaderFormat<Product> csvFormatProduct = CsvReaderFormat.forPojo(Product.class);

       FileSource<User> userFileSource =
                FileSource.forRecordStreamFormat(csvFormatUser, inputFolder)
                        .monitorContinuously(Duration.of(5, SECONDS))
                        .build();


        FileSource<Product> productFileSource =
                FileSource.forRecordStreamFormat(csvFormatProduct, inputFolder2)
                        .monitorContinuously(Duration.of(5, SECONDS))
                        .build();

        /******************************************************************************************
         * Creating Kafka sinks
         ******************************************************************************************/

        Properties properties = new Properties();
        properties.setProperty("transaction.timeout.ms", "7200");

        String topic3 = "enrichedTransactions";

        KafkaSink<EnrichedTransactionRecord> sink =
                KafkaSink.<EnrichedTransactionRecord>builder()
                        .setBootstrapServers(broker)
                        .setKafkaProducerConfig(properties)
                        .setRecordSerializer(new EnrichedTransactionRecordSerializationSchema(topic3))
                        .setTransactionalIdPrefix("jans2")
                        .build();

        /******************************************************************************************
         * Reading from Kafka data source
         ******************************************************************************************/

        DataStream<TransactionRecord> records =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        /******************************************************************************************
         * Reading from CSV data source
         ******************************************************************************************/

         DataStreamSource<User> usersCSV =
                env.fromSource(userFileSource, WatermarkStrategy.noWatermarks(), "Users File");

        DataStreamSource<Product> productCSV =
                env.fromSource(productFileSource, WatermarkStrategy.noWatermarks(), "Product File");


        /******************************************************************************************
         * Processing begins...
         ******************************************************************************************/

        // initiate Tables from data streams
        Table transactionsTable = tEnv.fromDataStream(records);
        Table usersTable = tEnv.fromDataStream(usersCSV);
        Table productsTable = tEnv.fromDataStream(productCSV);

        // join data together
        Table resultTable = transactionsTable
               .join(usersTable)
                .where($("user_id").isEqual($("customer_id")))
                .join(productsTable)
                .where($("product_id").isEqual($("prod_id")));

        // create stream out of result table
        DataStream<Row> tableRecords =
                tEnv.toChangelogStream(resultTable);

        // map correct fields
        DataStream<EnrichedTransactionRecord> enrichedRecords = tableRecords.map(tr -> new EnrichedTransactionRecord(
                (Instant)tr.getField("time"),
                (String)tr.getField("id"),
                (String)tr.getField("user_id"),
                (String)tr.getField("name"),
                (String)tr.getField("mail"),
                (String)tr.getField("product_id"),
                (String)tr.getField("product"),
                (String)tr.getField("category"),
                (Integer)tr.getField("amount"),
                (float)tr.getField("price"),
                ((float)tr.getField("price"))*((Integer)tr.getField("amount"))));


        enrichedRecords.sinkTo(sink);
        enrichedRecords.print();

        /******************************************************************************************
         * Executing and printing the results
         ******************************************************************************************/


        // Execute program, beginning computation.
        env.execute("Flink Java API Skeleton");
    }

}
