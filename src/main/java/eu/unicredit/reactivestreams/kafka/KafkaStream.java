package eu.unicredit.reactivestreams.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * Fluent KafkaPublisher/Subscriber builder.
 *
 * A {@link KafkaPublisher} republishes a Kafka topic as a stream of {@code ConsumerRecord<K,V>}

 * A {@link KafkaSubscriber} pushes onto Kafka a stream of {@code ProducerRecord<K,V>}
 *
 * This fluent builder onstructs Publishers and Subscribers dispatching via single-thread executor.
 * Different executor constructors can be provided using {@link #withExecutor(Supplier)}.
 *
 * Publishers subscribe a Kafka topic and poll with a {@link #DEFAULT_TIMEOUT} of 100ms.
 * A different timeout interval can be given using {@link #publisher(String, long)}.
 * e.g., a subscriber with custom executor and 1000ms poll interval:
 *
 * <code><pre>
 *     KafkaStream.of(props)
 *          .withExecutor(Executors::newCachedThreadPool)
 *          .publisher("my-topic", 1000)
 * </pre></code>
 *
 *
 *
 *
 */
public class KafkaStream {
    private static final long DEFAULT_TIMEOUT = 100;
    /**
     * Return an instance of the builder with the given Kafka config properties
     */
    public static KafkaStream of(Properties props) { return new KafkaStream(props); }

    private final Properties properties;
    private Supplier<? extends Executor> executorSupplier = Executors::newSingleThreadExecutor;

    private KafkaStream(final Properties properties) { this.properties = properties; }

    /**
     * Provide a different executor constructor; default is java.util.concurrent.Executors::newSingleThreadExecutor
     */
    public KafkaStream withExecutor(Supplier<? extends Executor> executor) { this.executorSupplier = executor; return this; }

    /**
     * @param topic the topic to subscribe
     * @param <K> key type
     * @param <V> value type
     */
    public <K,V> KafkaPublisher<K,V> publisher(String topic) { return publisher(topic, DEFAULT_TIMEOUT); }

    /**
     *
     * @param topic the topic to subscribe
     * @param timeout timeout interval in milliseconds
     * @param <K> key type
     * @param <V> value type
     */
    public <K,V> KafkaPublisher<K,V> publisher(String topic, long timeout) {
        final KafkaConsumer<K, V> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));
        return new KafkaPublisher<>(consumer, timeout, executorSupplier.get());
    }

    public <K,V> KafkaSubscriber<K,V> subscriber() {
        return new KafkaSubscriber<>(new KafkaProducer<>(properties), executorSupplier.get());
    }
}
