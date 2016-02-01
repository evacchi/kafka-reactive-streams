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
        return new KafkaSubscriber<Long,Long>(mockProducer) {
            @Override
            public void onSubscribe(final Subscription s) {
                super.onSubscribe(s);

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
            public void onNext(ProducerRecord<Long,Long> element) {
                super.onNext(element);
                probe.registerOnNext(element);
            }

            @Override
            public void onError(Throwable cause) {
                super.onError(cause);
                probe.registerOnError(cause);
            }

            @Override
            public void onComplete() {
                super.onComplete();
                probe.registerOnComplete();
            }
        };
    }
}
