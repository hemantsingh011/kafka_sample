package com.sample.kafka.tutorial1;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class ProducerWithCallback {
    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

    public static void main(String[] args) throws InterruptedException {
        // create producer properties
        final Logger logger = LoggerFactory.getLogger("ProducerWithCallback");
        final CountDownLatch latch = new CountDownLatch(1);
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create a producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("firstTopic", "hello world");
        //send data
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                logger.info(recordMetadata.toString());
                latch.countDown();
            }
        });
        latch.await();
        producer.flush();
        producer.close();
    }
}
