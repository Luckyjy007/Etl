package etl.binlog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumerConf {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerConf.class);
    private static Map kafkaParams = null;

    public static synchronized Map getKafkaParams() {
        if (kafkaParams == null) {
            kafkaParams = new HashMap<String, String>(10);
        }
        InputStream inputStream = KafkaConsumerConf.class.getClassLoader().getResourceAsStream("kafkaconsumer.properties");
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
