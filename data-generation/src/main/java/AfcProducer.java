import afc.AfcRecord;
import com.alibaba.fastjson.JSON;
import lombok.Getter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import util.KafkaUtil;

import java.io.IOException;

@Getter
public class AfcProducer {
    private KafkaProducer<String, String> producer;
    public static final String TP_AFC = "TP_AFC";

    public AfcProducer() throws IOException {
        this.producer = KafkaUtil.getKafkaProducer();
    }

    public void sendAfc(AfcRecord afcRecord) {
        ProducerRecord<String, String> record = new ProducerRecord<>(TP_AFC, JSON.toJSONString(afcRecord));
        producer.send(record);
    }
}
