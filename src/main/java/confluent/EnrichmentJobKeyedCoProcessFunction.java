package confluent;

import confluent.records.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Properties;

import static java.time.temporal.ChronoUnit.SECONDS;


public class EnrichmentJobKeyedCoProcessFunction {

    public static void main(String[] args) throws Exception {

        /******************************************************************************************
         * Getting the environment
         ******************************************************************************************/
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


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

        // join data together
        DataStream<EnrichedTransactionRecord> enrichedRecords = records
                .keyBy((KeySelector<TransactionRecord, String>) record -> record.user_id)
                .connect(usersCSV.keyBy((KeySelector<User, String>) user -> user.customer_id))
                .process(new JoinTransactionWithUser())
                .keyBy((KeySelector<EnrichedTransactionRecord, String>) record -> record.productId)
                .connect(productCSV.keyBy((KeySelector<Product, String>) product -> product.prod_id))
                .process(new JoinTransactionUserWithProduct());

        enrichedRecords.sinkTo(sink);

        enrichedRecords.print();

        // Execute the Flink job
        env.execute("Flink DataStream API Example");
    }

    /******************************************************************************************
     * KeyedCoProcessFunction to enrich transactions with user data
     ******************************************************************************************/
    // INPUT Key Type, TransactionRecord, User
    // OUTPUT EnrichedTransactionRecord without Product Info
    public static class JoinTransactionWithUser extends
            KeyedCoProcessFunction<String, TransactionRecord, User, EnrichedTransactionRecord> {

        // state to store users
        MapState<String, User> usersState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // initialize state
            MapStateDescriptor<String, User> usersStateDescriptor = new MapStateDescriptor<>(
                    "usersState", String.class, User.class);
            usersState = getRuntimeContext().getMapState(usersStateDescriptor);
        }

        @Override
        public void processElement1(TransactionRecord record, Context ctx, Collector<EnrichedTransactionRecord> out) throws Exception {
            // get user from state
            User user = usersState.get(record.user_id);
            // if user is found
            if (user != null) {
                // join data and create a new enriched record
                out.collect(new EnrichedTransactionRecord (
                        record.time, record.id, record.user_id, user.name, user.mail, record.product_id, null, null, record.amount, 0, 0));
            }
        }

        @Override
        public void processElement2(User user, Context ctx, Collector<EnrichedTransactionRecord> out) throws Exception {
            // store new user in stat
            usersState.put(user.customer_id, user);
        }
    }

    /******************************************************************************************
     * KeyedCoProcessFunction to enrich transactions with product data
     ******************************************************************************************/
    // INPUT Key Type, EnrichedTransactionRecord, Product
    // OUTPUT EnrichedTransactionRecord with Product Info
    public static class JoinTransactionUserWithProduct
            extends KeyedCoProcessFunction<String, EnrichedTransactionRecord, Product, EnrichedTransactionRecord> {

        // state to store products
        MapState<String, Product> productsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // initialize products state
            MapStateDescriptor<String, Product> productsStateDescriptor = new MapStateDescriptor<>(
                    "productsState", String.class, Product.class);
            productsState = getRuntimeContext().getMapState(productsStateDescriptor);
        }

        @Override
        public void processElement1(EnrichedTransactionRecord record, Context ctx, Collector<EnrichedTransactionRecord> out) throws Exception {
            // get product from state
            Product product = productsState.get(record.productId);
            // if product is found
            if (product != null) {
                // add product data to the enriched record
                record.product = product.product;
                record.category = product.category;
                record.unitPrice = product.price;
                record.totalPrice = record.amount * product.price;
                out.collect(record);
            }
        }

        @Override
        public void processElement2(Product product, Context ctx, Collector<EnrichedTransactionRecord> out) throws Exception {
            // store new product in state
            productsState.put(product.prod_id, product);
        }
    }
}


