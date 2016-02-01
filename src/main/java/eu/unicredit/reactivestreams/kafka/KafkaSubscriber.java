package eu.unicredit.reactivestreams.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.Executor;

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

