package eu.unicredit.reactivestreams.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

public class KafkaStream {
    private static final long DEFAULT_TIMEOUT = 100;

    private final Properties properties;
    private Supplier<? extends Executor> executorSupplier = Executors::newSingleThreadExecutor;

    public KafkaStream(final Properties properties) {
        this.properties = properties;
    }
    public KafkaStream withExecutor(Supplier<? extends Executor> executor) { this.executorSupplier = executor; return this; }

    public <K,V> KafkaPublisher<K,V> publisher(String topic) {
        return publisher(topic, DEFAULT_TIMEOUT);
    }
    public <K,V> KafkaPublisher<K,V> publisher(String topic, long timeout) {
        final KafkaConsumer<K, V> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));
        consumer.seekToEnd();
        return new KafkaPublisher<>(consumer, timeout, executorSupplier.get());
    }

    public <K,V> KafkaSubscriber<K,V> subscriber(String topic) {
        return new KafkaSubscriber<>(new KafkaProducer<>(properties), executorSupplier.get());
    }
}
