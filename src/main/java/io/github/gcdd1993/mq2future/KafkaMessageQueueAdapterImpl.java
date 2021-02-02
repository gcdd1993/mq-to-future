package io.github.gcdd1993.mq2future;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Kafka实现
 *
 * @author gcdd1993
 * @date 2021/2/1
 * @since 1.0.0
 */
@Slf4j
public class KafkaMessageQueueAdapterImpl<REQ extends TraceSupport, RES extends TraceSupport>
        implements MessageQueueAdapter<REQ, RES> {
    private final Timer timer = new Timer("KAFKA-MESSAGE-QUEUE-ADAPTER-TIMER");
    private final Map<String, Worker> workerMap = new ConcurrentHashMap<>(256);
    private final SenderOptions<Object, REQ> senderOptions;
    private final ReceiverOptions<Object, RES> receiverOptions;
    private final RES timeoutTraceSupport;
    private final String producerTopic;
    private final String consumerTopic;
    private final Duration timeoutDuration;
    private static final String DEFAULT_CLIENT_ID = "KAFKA-MESSAGE-QUEUE-ADAPTER-CLIENT";

    public KafkaMessageQueueAdapterImpl(SenderOptions<Object, REQ> senderOptions,
                                        ReceiverOptions<Object, RES> receiverOptions,
                                        RES timeoutTraceSupport,
                                        String producerTopic,
                                        String consumerTopic) {
        this(senderOptions,
                receiverOptions,
                "_DEFAULT_KAFKA_MESSAGE_QUEUE_ADAPTER",
                timeoutTraceSupport,
                producerTopic,
                consumerTopic,
                Duration.ofMinutes(3L));
    }

    public KafkaMessageQueueAdapterImpl(SenderOptions<Object, REQ> senderOptions,
                                        ReceiverOptions<Object, RES> receiverOptions,
                                        String receiverGroupName,
                                        RES timeoutTraceSupport,
                                        String producerTopic,
                                        String consumerTopic,
                                        Duration timeoutDuration) {
        this.timeoutTraceSupport = timeoutTraceSupport;
        this.producerTopic = producerTopic;
        this.consumerTopic = consumerTopic;
        this.timeoutDuration = timeoutDuration;
        this.senderOptions = senderOptions;
        if (receiverOptions.groupId() == null) {
            receiverOptions.consumerProperty(ConsumerConfig.GROUP_ID_CONFIG, receiverGroupName);
        }
        if (receiverOptions.consumerProperty(ConsumerConfig.CLIENT_ID_CONFIG) == null) {
            receiverOptions.consumerProperty(ConsumerConfig.CLIENT_ID_CONFIG, DEFAULT_CLIENT_ID);
        }
        this.receiverOptions = receiverOptions;
        _startListen();
    }

    private void _startListen() {
        KafkaReceiver.create(receiverOptions.subscription(Collections.singletonList(consumerTopic)))
                .receive()
                .doOnNext(record -> {
                    try {
                        var offset = record.receiverOffset();
                        var traceId = record.value().getTraceId();
                        var worker = workerMap.get(traceId);
                        worker.complete(record.value());
                        offset.acknowledge();
                    } catch (Exception ex) {
                        log.error("consume record {} error.", record, ex);
                    }
                })
                .doOnError(ex -> {
                    log.error("receive record error.", ex);
                })
                .subscribe()
        ;
    }

    @Override
    public Mono<RES> send(REQ req) {
        return Mono.fromFuture(sendFuture(req));
    }

    @Override
    public CompletableFuture<RES> sendFuture(REQ req) {
        var traceId = UUID.randomUUID().toString();
        req.setTraceId(traceId);
        var worker = new Worker();
        KafkaSender.create(senderOptions)
                .send(Mono.just(SenderRecord.create(new ProducerRecord<>(producerTopic, req), null)))
                .doOnComplete(() -> {
                    workerMap.put(traceId, worker);
                    timer.schedule(worker, timeoutDuration.toMillis());
                })
                .subscribe()
        ;
        return worker.getFuture();
    }

    /**
     * 工作线程
     */
    @RequiredArgsConstructor
    private class Worker
            extends TimerTask {
        @Getter
        private final CompletableFuture<RES> future = new CompletableFuture<>();

        @Override
        public void run() {
            this.complete(timeoutTraceSupport);
        }

        public synchronized void complete(RES res) {
            var traceId = res.getTraceId();
            if (workerMap.containsKey(traceId)) {
                workerMap.remove(traceId);
                future.complete(res);
            } else {
                log.warn("cannot find any worker for trace id {}.", traceId);
            }
        }
    }
}
