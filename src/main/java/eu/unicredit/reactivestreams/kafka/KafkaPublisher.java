package eu.unicredit.reactivestreams.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.Iterator;
import java.util.concurrent.Executor;

/**
 * A {@link KafkaPublisher} republishes a Kafka topic as a stream of {@code ConsumerRecord<K,V>}
 *
 * @param <K> type of the Key in the ConsumerRecord
 * @param <V> type of the Value in the ConsumerRecord
 */
public class KafkaPublisher<K,V> implements Publisher<ConsumerRecord<K,V>> {
    private final AsyncIterablePublisher<ConsumerRecord<K,V>> delegate;
    private final Consumer<K, V> consumer;
    private final long timeout;

    public KafkaPublisher(final Consumer<K, V> consumer, final long timeout, final Executor executor) {
        this.consumer = consumer;
        this.timeout = timeout;
        this.delegate = new AsyncIterablePublisher<>(KafkaIterator::new, executor);
    }

    @Override public void subscribe(final Subscriber<? super ConsumerRecord<K, V>> subscriber) {
        this.delegate.subscribe(subscriber);
    }

    class KafkaIterator implements Iterator<ConsumerRecord<K,V>> {
        private Iterator<ConsumerRecord<K, V>> outstanding = requestMore();
        @Override public boolean hasNext() { return true; }
        @Override public ConsumerRecord<K, V> next() {
            while(!outstanding.hasNext()) { outstanding = requestMore(); }
            return outstanding.next();
        }
        private Iterator<ConsumerRecord<K, V>> requestMore() { return consumer.poll(timeout).iterator(); }
    };



}
