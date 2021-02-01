package com.github.gcdd1993.mq2future;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;

/**
 * @author gcdd1993
 * @date 2021/2/1
 * @since 1.0.0
 */
@Slf4j
class KafkaMessageQueueAdapterImplTest {
    private static final String CONSUMER_TOPIC = "kafka-message-queue-adapter-res";
    private static final String PRODUCER_TOPIC = "kafka-message-queue-adapter-req";

    @BeforeEach
    void setUp() {
        // 启动一个自动接收消息并回复消息的receiver
        var props = new HashMap<String, Object>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.3.123:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-producer-1");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KryoSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KryoSerializer.class);
        var senderOptions = SenderOptions.<Object, DemoResponse>create(props);
        var sender = KafkaSender.create(senderOptions);

        props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.3.123:9092");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer-1");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group-2");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KryoDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KryoDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        var receiverOptions = ReceiverOptions.<Object, DemoRequest>create(props)
                .subscription(Collections.singletonList(PRODUCER_TOPIC));

        var receiver = KafkaReceiver.create(receiverOptions);
        receiver
                .receiveAtmostOnce()
                .map(record -> {
                    var traceId = record.value().getTraceId();
                    var res = new DemoResponse();
                    res.setTraceId(traceId);
                    res.setTimeout(false);
                    res.setPayload(MessageFormat.format("received message {0}", record.value()));
                    return SenderRecord.create(new ProducerRecord<>(CONSUMER_TOPIC, res), null);
                })
                .as(sender::send)
                .doOnError(ex -> log.error("send failed.", ex))
                .delaySubscription(Duration.ofSeconds(10L)) // 延迟10s订阅
                .subscribe();
        log.info("test listener is started.");
    }

    @Test
    void send() {
        var props = new HashMap<String, Object>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.3.123:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-producer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KryoSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KryoSerializer.class);
        var senderOptions = SenderOptions.<Object, DemoRequest>create(props);

        props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.3.123:9092");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KryoDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KryoDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        var receiverOptions = ReceiverOptions.<Object, DemoResponse>create(props);
        receiverOptions
                .subscription(Collections.singletonList(CONSUMER_TOPIC));

        var adapter = new KafkaMessageQueueAdapterImpl<>(
                senderOptions,
                receiverOptions,
                DemoResponse.timeoutOne(),
                PRODUCER_TOPIC,
                CONSUMER_TOPIC
        );
        var req = new DemoRequest();
        var traceId = UUID.randomUUID().toString();
        req.setTraceId(traceId);
        adapter.send(req)
                .doOnError(e -> log.error("send failed.", e))
                .doOnNext(it -> {
                    log.info("receive res: {}", it);
                })
                .subscribe();
        for (; ; ) {
            // blocking
        }
    }

    @Test
    void sendFuture() {
    }

    @Data
    static class DemoRequest
            implements TraceSupport {
        private String traceId;
        private boolean timeout;
    }

    @Data
    static class DemoResponse
            implements TraceSupport {
        private String traceId;
        private boolean timeout;
        private String payload;

        public static DemoResponse timeoutOne() {
            var req = new DemoResponse();
            req.setTimeout(true);
            req.setTraceId(UUID.randomUUID().toString());
            return req;
        }
    }

}