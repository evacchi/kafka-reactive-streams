# Reactive Streams for Kafka

A [Reactive Streams](https://github.com/reactive-streams/reactive-streams-jvm)-compliant
connector for Kafka.

Example Usage:

```java
Properties props = new Properties()
props.put("bootstrap.servers", "localhost:9092");
props.put("group.id", "test");
// props.put("enable.auto.commit", "false");
props.put("auto.commit.interval.ms", "1000");
props.put("session.timeout.ms", "6000");
props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

// returns a Publisher<ConsumerRecord<Long, Double>>
final KafkaPublisher<Long, Double> publisher = KafkaStream.of(props).publisher("my-topic");

// e.g., using rx java 2.0, an stream that prints each key of each ConsumerRecord
Observable.fromPublisher(publisher).subscribe(t -> System.out.println(t.key()));

// returns a Subscriber<ProducerRecord<Long, Double>>
final KafkaSubscriber<Long, Double> subscriber = KafkaStream.of(props).subscriber("repub-topic");
// takes the previous stream, multiplies value by two, republishes to "repub-topic" under the same key
Observable.fromPublisher(publisher)
          .map(r -> new ProducerRecord("repub-topic", r.key(), r.value() * 2))
          .subscribe(subscriber);

```



