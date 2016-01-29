package eu.unicredit.reactivestreams.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.Iterator;

public class KafkaPublisher<K,V> implements Publisher<ConsumerRecord<K,V>> {
    private final KafkaConsumer<K,V> kafkaConsumer;
    private final long timeout;
    private KafkaSubscription subscription;
    private Subscriber<? super ConsumerRecord<K, V>> subscriber;
    private Iterator<ConsumerRecord<K, V>> outstanding;


    public KafkaPublisher(final KafkaConsumer<K, V> kafkaConsumer, final long timeout) {
        this.kafkaConsumer = kafkaConsumer;
        this.timeout = timeout;
    }

    @Override
    public void subscribe(final Subscriber<? super ConsumerRecord<K, V>> subscriber) {
        if (subscription != null) {
            subscriber.onError(new IllegalStateException("Multiple subscribers are currently not supported"));
            return;
        }
        this.outstanding = requestMore();
        this.subscription = new KafkaSubscription(this);
        this.subscriber = subscriber;
        this.subscriber.onSubscribe(this.subscription);
    }

    private Iterator<ConsumerRecord<K, V>> requestMore() {
        return kafkaConsumer.poll(timeout).iterator();
    }

    public void next(long n) {
        try {
            System.out.println(n);
            while (n > 0) {
                if (this.outstanding.hasNext()) {
                    subscriber.onNext(this.outstanding.next());
                    n--;
                } else {
                    System.out.println("no outstanding");
                    this.outstanding = requestMore();
                }
            }

            System.out.println("end");

        } catch (Throwable t) { subscriber.onError(t); }
    }
}
