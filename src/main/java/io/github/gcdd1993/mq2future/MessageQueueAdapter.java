package io.github.gcdd1993.mq2future;

import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;

/**
 * 将Message Queue的消息转换为
 * - {@link reactor.core.publisher.Mono}
 * - {@link java.util.concurrent.CompletableFuture}
 *
 * @author gcdd1993
 * @date 2021/2/1
 * @since 1.0.0
 */
public interface MessageQueueAdapter<REQ extends TraceSupport, RES extends TraceSupport>
        extends Cloneable {

    /**
     * 异步发送消息
     *
     * @param req 消息
     */
    Mono<RES> send(REQ req);

    /**
     * 异步发送消息
     *
     * @param req 消息
     */
    CompletableFuture<RES> sendFuture(REQ req);

}
