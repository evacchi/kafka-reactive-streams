/*
 * Copyright 2016 UniCredit S.p.A.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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
public class KafkaPublisherTest extends PublisherVerification<ConsumerRecord<Long, Double>> {

    MockConsumer<Long,Double> mockConsumer;
    TopicPartition topicPartition = new TopicPartition("topic", 0);

    public KafkaPublisherTest() {
        super(new TestEnvironment());
    }

    @Override public long maxElementsFromPublisher() {
        return super.publisherUnableToSignalOnComplete();
    }


    public KafkaPublisherTest(final TestEnvironment env) {
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
