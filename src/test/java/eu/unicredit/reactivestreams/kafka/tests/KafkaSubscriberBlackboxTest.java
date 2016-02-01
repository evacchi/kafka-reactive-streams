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
import java.util.concurrent.Executors;

@Test
public class KafkaSubscriberBlackboxTest extends SubscriberBlackboxVerification<ProducerRecord<Long,Long>> {
    final MockProducer<Long, Long> mockProducer;
    public KafkaSubscriberBlackboxTest() {
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
        return new KafkaSubscriber<Long,Long>(mockProducer, Executors.newSingleThreadExecutor());
    }
}
