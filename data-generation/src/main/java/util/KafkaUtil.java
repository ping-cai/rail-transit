package util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 加载kafka的配置工具
 */
public class KafkaUtil {
    /**
     * 通过读取Properties
     *
     * @return 获取数据库信息
     */
    public static KafkaProducer<String, String> getKafkaProducer() throws IOException {
        Properties properties = new Properties();
        InputStream resourceAsStream = KafkaUtil.class.getClassLoader().getResourceAsStream("kafka.properties");
        properties.load(resourceAsStream);
        return new KafkaProducer<>(properties);
    }
}
