package eu.unicredit.reactivestreams.kafka.tests;

import eu.unicredit.reactivestreams.kafka.KafkaSubscriber;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.SubscriberBlackboxVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.Test;

import java.util.Random;

@Test
public class KafkaSubscriberBlackboxTests extends SubscriberBlackboxVerification<ProducerRecord<Long,Long>> {
    final MockProducer<Long, Long> mockProducer;
    public KafkaSubscriberBlackboxTests() {
        super(new TestEnvironment());
        mockProducer = new MockProducer<Long, Long>(true, new LongSerializer(), new LongSerializer());
    }

    final Random random = new Random();
    @Override
    public ProducerRecord<Long, Long> createElement(final int element) {
        return new ProducerRecord<Long, Long>("foo", random.nextLong());
    }


    @Override
    public Subscriber<ProducerRecord<Long, Long>> createSubscriber() {
        return new KafkaSubscriber<Long,Long>(mockProducer);
    }
}
