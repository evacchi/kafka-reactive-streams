package eu.unicredit.reactivestreams.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Executors;

public class KafkaStream {
    private static final long DEFAULT_TIMEOUT = 100;

    private final Properties properties;
    public KafkaStream(final Properties properties) {
        this.properties = properties;
    }

    public <K,V> KafkaPublisher<K,V> publisher(String topic) {
        return publisher(topic, DEFAULT_TIMEOUT);
    }
    public <K,V> KafkaPublisher<K,V> publisher(String topic, long timeout) {
        final KafkaConsumer<K, V> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return new KafkaPublisher<>(consumer, timeout, Executors.newSingleThreadExecutor());
    }
    public <K,V> KafkaSubscriber<K,V> subscriber(String topic) {
        return new KafkaSubscriber<>(new KafkaProducer<K, V>(properties));
    }
}
