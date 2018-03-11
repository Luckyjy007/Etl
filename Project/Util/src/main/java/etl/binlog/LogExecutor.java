package etl.binlog;

import com.aqj.etl.test.SparkExecutor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

public class LogExecutor extends SparkExecutor {

    private static Set<String> ids;

    protected static void getMysqlFieldsPerMin(int minute){
        Executors.newFixedThreadPool(1)
                .execute(()->{
                    ids = getMysqlFields();
                    while (true){
                        try {
                            Thread.sleep(minute * 60 * 1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        ids= getMysqlFields();
                    }
                });
    }


    @Override
    public Map<String, String> configMap() {
        Map<String,String> config = new HashMap<>();
        config.put("mongoUri","mongo:21.11.2");
        return config;
    }


    public void  doTask(String[] topics){

        JavaInputDStream<ConsumerRecord<String, byte[]>> directStream = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(Arrays.asList(topics), KafkaConsumerConf.getKafkaParams()));
        streamingContext.checkpoint("/user/jiangyun/checkpoint");
        JavaDStream<ConsumerRecord<String, byte[]>> repartition = directStream.repartition(3);

        loadMysqlTable("binlog","AAA");
        Set<String> ids = getMysqlFields();
        spark.udf().register("validate",(String taskId)-> ids.contains(taskId), DataTypes.BooleanType);
         sql("select f1 from AAA where validate(f2)");




    }



}
