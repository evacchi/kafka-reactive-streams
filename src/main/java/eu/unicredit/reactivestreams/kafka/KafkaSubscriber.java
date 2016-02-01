package eu.unicredit.reactivestreams.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class KafkaSubscriber<K, V> implements Subscriber<ProducerRecord<K, V>> {
    private final Producer<K,V> kafkaProducer;
    private Subscription subscription;

    public KafkaSubscriber(final Producer<K, V> kafkaProducer) { this.kafkaProducer = kafkaProducer; }

    @Override
    public void onSubscribe(final Subscription subscription) {
        if (this.subscription != null) {
            subscription.cancel();
            return;
        }
        this.subscription = subscription;
        this.subscription.request(1);
    }

    @Override
    public void onNext(final ProducerRecord<K, V> producerRecord) {
        this.kafkaProducer.send(producerRecord);
        this.subscription.request(1);
    }

    @Override
    public void onError(final Throwable throwable) {
        if (throwable == null) throw null;
        this.kafkaProducer.close();
    }

    @Override
    public void onComplete() {
        this.kafkaProducer.close();
    }
}

