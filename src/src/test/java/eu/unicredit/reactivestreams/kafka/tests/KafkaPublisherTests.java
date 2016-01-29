package eu.unicredit.reactivestreams.kafka.tests;

import eu.unicredit.reactivestreams.kafka.KafkaPublisher;
import eu.unicredit.reactivestreams.kafka.KafkaStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;

@Test
public class KafkaPublisherTests extends PublisherVerification<ConsumerRecord<Long, Double>> {

    MockConsumer<Long,Double> mockConsumer;
    TopicPartition topicPartition = new TopicPartition("topic", 0);

    public KafkaPublisherTests() {
        super(new TestEnvironment());
    }

    @Override public long maxElementsFromPublisher() {
        return super.publisherUnableToSignalOnComplete();
    }


    public KafkaPublisherTests(final TestEnvironment env) {
        super(env);
    }

    @Override public Publisher<ConsumerRecord<Long, Double>> createPublisher(final long l) {
        long nRecords = 100;

        mockConsumer = new MockConsumer<Long, Double>(OffsetResetStrategy.LATEST);
        mockConsumer.assign(Arrays.asList(topicPartition));
        final HashMap<TopicPartition, Long> topicPartitionLongHashMap = new HashMap<>();
        topicPartitionLongHashMap.put(topicPartition, 0L);
        mockConsumer.updateBeginningOffsets(topicPartitionLongHashMap);
        topicPartitionLongHashMap.put(topicPartition, nRecords - 1);
        mockConsumer.updateEndOffsets(topicPartitionLongHashMap);
        final Random random = new Random();
        for (int i = 0; i < nRecords; i++)
            mockConsumer.addRecord(
                    new ConsumerRecord<Long, Double>(
                            topicPartition.topic(),
                            topicPartition.partition(),
                            i,
                            random.nextLong(),
                            random.nextDouble()));

        return new KafkaPublisher<Long, Double>(mockConsumer, 100, Executors.newSingleThreadExecutor());
    }

    @Override
    public Publisher<ConsumerRecord<Long, Double>> createFailedPublisher() {
        return null;
    }
}
