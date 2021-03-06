package etl.binlog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/*
Create by jiangyun on 2017/12/20
*/
public class KafkaProducerConf {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerConf.class);
    private static Map kafkaParams = null;

    public static synchronized Map getKafkaParams() {
        if (kafkaParams == null) {
            kafkaParams = new HashMap<String, String>(10);
        }
        InputStream inputStream = KafkaProducerConf.class.getClassLoader().getResourceAsStream("kafkaproducer.properties");
        Properties p = new Properties();
        try {
            p.load(inputStream);
        } catch (IOException e) {
            //  e.printStackTrace();
            logger.error("Error loading configuration file information");
            throw new RuntimeException(e);
        }
        kafkaParams.putAll(p);
        return kafkaParams;
    }
}
