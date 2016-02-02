# Reactive Streams for Kafka

A lightweight [Reactive Streams](https://github.com/reactive-streams/reactive-streams-jvm)-compliant
connector for Kafka.

## Dependencies

You can currently depend on `kafka-reactive-streams` using Jitpack.
E.g., with Maven:

```xml
    <repositories>
        <repository>
            <id>jitpack.io</id>
            <url>https://jitpack.io</url>
        </repository>
    </repositories>

    <dependencies>
        ...
        <dependency>
            <groupId>com.github.unicredit</groupId>
            <artifactId>kafka-reactive-streams</artifactId>
            <version>0.1.1</version>
        </dependency>
        ...
    </dependencies>
```

## Example Usage:

```java
Properties props = new Properties()
// ... kafka config ...

// returns a Publisher<ConsumerRecord<Long, Double>>
final KafkaPublisher<Long, Double> publisher = KafkaStream.of(props).publisher("my-topic");

// e.g., using rx java 2.0 (currently unstable), an stream that prints each key of each ConsumerRecord
Observable.fromPublisher(publisher).subscribe(t -> System.out.println(t.key()));

// returns a Subscriber<ProducerRecord<Long, Double>>
final KafkaSubscriber<Long, Double> subscriber = KafkaStream.of(props).subscriber("repub-topic");
// takes the previous stream, multiplies value by two, republishes to "repub-topic" under the same key
Observable.fromPublisher(publisher)
          .map(r -> new ProducerRecord("repub-topic", r.key(), r.value() * 2))
          .subscribe(subscriber);
```

Custom publishers and subscribers can be created using the fluent `KafkaStream` builder.
E.g., a publisher that runs on a cached thread pool (default: single-thread)
with custom poll interval (default is 100ms):

```java
final KafkaPublisher<Long, Double> =
        KafkaStream.of(props)
            .withExecutor(Executors::newCachedThreadPool)
            .publisher("my-topic", 1000) // 1000ms poll
```


