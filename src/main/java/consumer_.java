import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.parquet.format.UUIDType;
import org.apache.spark.SparkConf;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.util.parsing.combinator.token.Tokens;

import javax.xml.crypto.Data;
import java.io.EOFException;
import java.util.*;

import static org.apache.spark.sql.functions.*;

public class consumer_ {



    public static void run_consumer()
    {

            String GEN_UUID = "id";
            SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("Net");
            //JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

            Map<String, Object> kafkaParams = new HashMap<>();

            {
                kafkaParams.put("bootstrap.servers", "localhost:9092,anotherhost:9092");
                kafkaParams.put("key.deserializer", StringDeserializer.class);
                kafkaParams.put("value.deserializer", StringDeserializer.class);
                kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
                kafkaParams.put("auto.offset.reset", "latest");
                kafkaParams.put("enable.auto.commit", false);
            }

            Collection<String> topics = Arrays.asList("test");

            /*JavaInputDStream<ConsumerRecord<String, String>> stream =
                    KafkaUtils.createDirectStream(
                            jssc,
                            LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                    );

            stream.foreachRDD(input -> {
                input.foreach(str -> {
                    System.out.println("Stream:" + str.value());
                });

            });

            //SPARK_STREAMING
            //jssc.start();
            //jssc.awaitTermination();
*/
            SparkSession spark = SparkSession.builder()
                    .master("local")
                    .appName("SparkSessionEx")
                    .config("hive.metastore.warehouse.dir", "file:/home/daria/IdeaProjects/test/spark-warehouse")
                    .getOrCreate();


            Properties properties = new Properties();
            properties.setProperty("user", "daria");
            properties.put("stringtype", "unspecified");
            Dataset<Row> jdbc = spark.read()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094")
                    .option("subscribe", "test")
                    .load();

            spark.udf().register(GEN_UUID,(String s)->
            s = UUID.randomUUID().toString()
        , DataTypes.StringType);


        Dataset<Row> v = jdbc.selectExpr("CAST(value AS STRING)").withColumnRenamed("value","data");


                 Dataset<Row> values = v.withColumn("id",callUDF(GEN_UUID,lit((("data")))));



        values.write()
                    .mode(SaveMode.Append)
                    .jdbc("jdbc:postgresql://0.0.0.0:5432/demo", "test", properties);


        }



}
