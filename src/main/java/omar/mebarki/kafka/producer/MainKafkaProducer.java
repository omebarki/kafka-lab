package omar.mebarki.kafka.producer;

import omar.mebarki.kafka.config.CommonConfig;
import omar.mebarki.kafka.config.Constants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MainKafkaProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = CommonConfig.loadProducerConfig();
        KafkaProducer<Long, String> producer = new KafkaProducer<Long, String>(props);

        for (int i = 0; i < 10; i++) {
            ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(Constants.TOPIC_NAME, Long.valueOf(i), "Hello " + i);
            Future<RecordMetadata> metatdataFuture = producer.send(record);
            RecordMetadata recordMetadata = metatdataFuture.get();
            System.out.println(recordMetadata.toString());
        }

    }
}
