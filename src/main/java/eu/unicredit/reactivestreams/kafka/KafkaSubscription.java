package eu.unicredit.reactivestreams.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Created by evacchi on 28/01/16.
 */
public class KafkaSubscription<K, V> implements Subscription {
    private final KafkaPublisher<K, V> kafkaPublisher;

    public KafkaSubscription(final KafkaPublisher<K,V> kafkaPublisher) {
        this.kafkaPublisher = kafkaPublisher;
    }


    @Override
    public void request(final long n) {
        kafkaPublisher.next(n);
    }

    @Override
    public void cancel() {

    }
}
