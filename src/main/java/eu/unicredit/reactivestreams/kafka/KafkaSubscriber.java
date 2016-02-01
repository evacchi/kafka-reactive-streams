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

package eu.unicredit.reactivestreams.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.Executor;

/**
 *
 * A {@link KafkaSubscriber} pushes onto Kafka a stream of {@code ProducerRecord<K,V>}.
 *
 * Subscribers forward ProducerRecords to the underlying KafkaProducer instance.
 *
 * @param <K> type of the Key in the ConsumerRecord
 * @param <V> type of the Value in the ConsumerRecord
 */
public class KafkaSubscriber<K, V> extends AsyncSubscriber<ProducerRecord<K,V>> {
    private final Producer<K,V> kafkaProducer;

    public KafkaSubscriber(final Producer<K, V> kafkaProducer, final Executor executor) {
        super(executor);
        this.kafkaProducer = kafkaProducer;
    }

    @Override protected boolean whenNext(final ProducerRecord<K, V> element) {
        kafkaProducer.send(element);
        return true;
    }
}

