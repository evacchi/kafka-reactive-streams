package eu.unicredit.reactivestreams.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaPublisher<K,V> extends AsyncIterablePublisher<ConsumerRecord<K,V>> {
    public KafkaPublisher(final Consumer<K, V> consumer, final long timeout, final Executor executor) {
        super(() -> new Iterator<ConsumerRecord<K, V>>() {
            Iterator<ConsumerRecord<K, V>> outstanding = requestMore();

            @Override public boolean hasNext() { return true; }

            @Override public ConsumerRecord<K, V> next() {
                while(!outstanding.hasNext()) { requestMore(); }
                return outstanding.next();
            }

            private Iterator<ConsumerRecord<K, V>> requestMore() {
                return consumer.poll(timeout).iterator();
            }
        }, executor);

    }


}
