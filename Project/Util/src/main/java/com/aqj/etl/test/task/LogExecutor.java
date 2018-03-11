package com.aqj.etl.test.task;

import com.aqj.etl.test.SparkExecutor;
import org.apache.spark.sql.types.DataTypes;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

public class LogExecutor extends SparkExecutor {

    private static Set<String> ids;

    protected static void getMysqlIds2(){
        Executors.newFixedThreadPool(1)
                .execute(()->{
                    ids = getMysqlFields();
                    while (true){
                        try {
                            Thread.sleep(10 * 60 * 1000);
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


    public void  doTask(){
        loadMysqlTable("binlog","AAA");
        Set<String> ids = getMysqlFields();
        spark.udf().register("validate",(String taskId)-> ids.contains(taskId), DataTypes.BooleanType);
        sql("select f1 from AAA where validate(f2)");
    }


    public static void main(String[] args) {
        LogExecutor executor = new LogExecutor();
        executor.doTask();


    }

}
