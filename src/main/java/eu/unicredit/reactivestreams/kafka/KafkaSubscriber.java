package eu.unicredit.reactivestreams.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class KafkaSubscriber<K, V> implements Subscriber<ProducerRecord<K, V>> {
    private final KafkaProducer<K,V> kafkaProducer;

    public KafkaSubscriber(final KafkaProducer<K, V> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void onSubscribe(final Subscription subscription) {

    }

    @Override
    public void onNext(final ProducerRecord<K, V> producerRecord) {
        this.kafkaProducer.send(producerRecord);
    }

    @Override
    public void onError(final Throwable throwable) {
        this.kafkaProducer.close();
    }

    @Override
    public void onComplete() {
        this.kafkaProducer.close();
    }
}

