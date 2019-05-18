package omar.mebarki.kafka.consumer;

import omar.mebarki.kafka.config.CommonConfig;
import omar.mebarki.kafka.config.Constants;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class MainKafkaConsumer {
    public static void main(String[] args) {
        Properties props = CommonConfig.loadConsumerConfig();

        KafkaConsumer consumer = new KafkaConsumer(props);
        consumer.subscribe(Collections.<String>singletonList(Constants.TOPIC_NAME), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                partitions.forEach(partition -> {
                    consumer.seek(partition, 6L);

                });
            }
        });
        consumer.poll(Duration.ofMillis(0));
        while (true) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            consumerRecords.forEach(record -> {
                System.out.println("Record Key " + record.key());
                System.out.println("Record value " + record.value());
                System.out.println("Record partition " + record.partition());
                System.out.println("Record offset " + record.offset());
                System.out.println("---------------------");
            });
            consumer.commitAsync();
        }
    }

    private static void startFromBegening(KafkaConsumer consumer) {
        consumer.seekToBeginning(consumer.assignment());
    }
}
