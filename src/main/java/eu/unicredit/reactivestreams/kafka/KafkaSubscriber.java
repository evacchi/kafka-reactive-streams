package eu.unicredit.reactivestreams.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.Executor;

/**
 *
 * A {@link KafkaSubscriber} pushes onto Kafka a stream of {@code ProducerRecord<K,V>}.
 *
 * Subscribers forward ProducerRecords to the underlying KafkaProducer instance.
 *
 * @param <K> type of the Key in the ConsumerRecord
 * @param <V> type of the Value in the ConsumerRecord
 */
public class KafkaSubscriber<K, V> extends AsyncSubscriber<ProducerRecord<K,V>> {
    private final Producer<K,V> kafkaProducer;

    public KafkaSubscriber(final Producer<K, V> kafkaProducer, final Executor executor) {
        super(executor);
        this.kafkaProducer = kafkaProducer;
    }

    @Override protected boolean whenNext(final ProducerRecord<K, V> element) {
        kafkaProducer.send(element);
        return true;
    }
}

