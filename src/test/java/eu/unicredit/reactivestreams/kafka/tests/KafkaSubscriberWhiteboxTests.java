package eu.unicredit.reactivestreams.kafka.tests;

import eu.unicredit.reactivestreams.kafka.KafkaSubscriber;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.reactivestreams.tck.SubscriberWhiteboxVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.Test;

import java.util.Random;
import java.util.concurrent.Executors;

@Test
public class KafkaSubscriberWhiteboxTests extends SubscriberWhiteboxVerification<ProducerRecord<Long,Long>> {
    final MockProducer<Long, Long> mockProducer;
    public KafkaSubscriberWhiteboxTests() {
        super(new TestEnvironment());
        mockProducer = new MockProducer<Long, Long>(true, new LongSerializer(), new LongSerializer());
    }

    final Random random = new Random();
    @Override
    public ProducerRecord<Long, Long> createElement(final int element) {
        return new ProducerRecord<Long, Long>("foo", random.nextLong());
    }


    @Override
    public Subscriber<ProducerRecord<Long,Long>> createSubscriber(final WhiteboxSubscriberProbe<ProducerRecord<Long,Long>> probe) {
        return new Subscriber<ProducerRecord<Long, Long>>() {
            KafkaSubscriber<Long, Long> delegate = new KafkaSubscriber<>(mockProducer, Executors.newSingleThreadExecutor());
            @Override
            public void onSubscribe(final Subscription s) {
                delegate.onSubscribe(s);
                probe.registerOnSubscribe(new SubscriberPuppet() {
                    @Override
                    public void triggerRequest(long elements) {
                        s.request(elements);
                    }

                    @Override
                    public void signalCancel() {
                        s.cancel();
                    }
                });
            }

            @Override
            public void onNext(final ProducerRecord<Long, Long> el) {
                delegate.onNext(el);
                probe.registerOnNext(el);
            }

            @Override
            public void onError(final Throwable t) {
                delegate.onError(t);
                probe.registerOnError(t);
            }

            @Override
            public void onComplete() {
                delegate.onComplete();
                probe.registerOnComplete();
            }
        };
    }
}
