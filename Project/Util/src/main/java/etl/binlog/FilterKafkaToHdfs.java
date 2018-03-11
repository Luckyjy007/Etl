package etl.binlog;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Arrays;

public class FilterKafkaToHdfs  {

    private transient SparkSession spark;


    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("chinalbs").setMaster("yarn-client");
        // SparkConf sparkConf = new SparkConf().setAppName("chinalbs").setMaster("spark://172.169.0.89:7077");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Duration.apply(1000));
        SQLContext sqlContext = new SQLContext(sparkContext);


        String[] topics = {"",""};
        JavaInputDStream<ConsumerRecord<String, byte[]>> directStream = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(Arrays.asList(topics), KafkaConsumerConf.getKafkaParams()));
        streamingContext.checkpoint("/user/jiangyun/checkpoint");
        JavaDStream<ConsumerRecord<String, byte[]>> repartition = directStream.repartition(3);




    }


}
