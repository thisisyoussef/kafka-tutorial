package io.conduktor.demos;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.admin.NewPartitions;


public class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());
    public static void main(String[] args) {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            adminClient.createPartitions(Collections.singletonMap("demo_java", NewPartitions.increaseTo(3))).all().get();
        } catch (Exception e) {
            // Handle exception as needed
        }
        log.info("Starting ProducerDemo");

        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");


        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());


       // properties.setProperty("partitioner.class", UniformStickyPartitioner.class.getName());

        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j < 2; j++) {

            for (int i = 0; i < 10; i++) {
                String topic = "demo_java";
                String key = "id_" + i;
                String value = "Hello " + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                //send data
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            log.info("Received new metadata \n" +
                                    "Key " + key + "\n" +
                                    "Partition " + recordMetadata.partition() + "\n");
                        } else {
                            log.error("Failed to send message to kafka", e);
                        }
                    }
                });


            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            }
        }

        producer.flush();

        //flush and close the producer
        producer.close();
    }
}
