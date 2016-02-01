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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.Iterator;
import java.util.concurrent.Executor;

/**
 * A {@link KafkaPublisher} republishes a Kafka topic as a stream of {@code ConsumerRecord<K,V>}
 *
 * @param <K> type of the Key in the ConsumerRecord
 * @param <V> type of the Value in the ConsumerRecord
 */
public class KafkaPublisher<K,V> implements Publisher<ConsumerRecord<K,V>> {
    private final AsyncIterablePublisher<ConsumerRecord<K,V>> delegate;
    private final Consumer<K, V> consumer;
    private final long timeout;

    public KafkaPublisher(final Consumer<K, V> consumer, final long timeout, final Executor executor) {
        this.consumer = consumer;
        this.timeout = timeout;
        this.delegate = new AsyncIterablePublisher<>(KafkaIterator::new, executor);
    }

    @Override public void subscribe(final Subscriber<? super ConsumerRecord<K, V>> subscriber) {
        this.delegate.subscribe(subscriber);
    }

    class KafkaIterator implements Iterator<ConsumerRecord<K,V>> {
        private Iterator<ConsumerRecord<K, V>> outstanding = requestMore();
        @Override public boolean hasNext() { return true; }
        @Override public ConsumerRecord<K, V> next() {
            while(!outstanding.hasNext()) { outstanding = requestMore(); }
            return outstanding.next();
        }
        private Iterator<ConsumerRecord<K, V>> requestMore() { return consumer.poll(timeout).iterator(); }
    };



}
