package eu.unicredit.reactivestreams.kafka.tests;

import eu.unicredit.reactivestreams.kafka.KafkaStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.Test;

import java.util.Properties;

@Test
public class KafkaPublisherTests extends PublisherVerification<ConsumerRecord<Long, byte[]>> {
    final String topic = "random-ts";
    private final Properties props = new Properties();
    {
        props.put("bootstrap.servers", "192.168.99.100:9092");
        props.put("group.id", "test");
        // props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "6000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    }

    public KafkaPublisherTests() {
        super(new TestEnvironment());
    }



    public KafkaPublisherTests(final TestEnvironment env) {
        super(env);
    }

    @Override
    public Publisher<ConsumerRecord<Long, byte[]>> createPublisher(final long l) {
        return new KafkaStream(props).publisher(topic);
    }

    @Override
    public Publisher<ConsumerRecord<Long, byte[]>> createFailedPublisher() {
        return null;
    }
}
