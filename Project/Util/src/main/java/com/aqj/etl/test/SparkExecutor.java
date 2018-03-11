package com.aqj.etl.test;

import com.aqj.etl.test.dao.MysqlDaoSupport;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class SparkExecutor implements Serializable {

    protected transient SparkSession spark;
    protected transient JavaSparkContext jsc;
    protected transient JavaStreamingContext streamingContext;
    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    protected void init() {
        SparkSession.Builder builder = SparkSession.builder();
        configMap().forEach((key, val) -> builder.config(key, val));
        spark = builder.enableHiveSupport().getOrCreate();
        jsc = new JavaSparkContext(spark.sparkContext());
        streamingContext= new JavaStreamingContext(jsc, Duration.apply(1000));
    }

    public abstract Map<String, String> configMap();


    protected static Set<String> getMysqlFields() {
        List<String> ids = new MysqlDaoSupport()
                .getJdbcTemplate()
                .queryForList("SELECT id from tb", String.class);
        return new HashSet<>(ids);
    }


    protected void loadMysqlTable(String table, String tmpTable) {
        spark.read().jdbc("", table, null).createOrReplaceTempView(tmpTable);
    }

    public Dataset<Row> sql(String sql) {
        logger.warn(sql);
        return spark.sql(sql);
    }

    public SparkExecutor() {
        init();
    }

}
