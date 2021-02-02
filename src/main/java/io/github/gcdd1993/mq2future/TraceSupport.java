package io.github.gcdd1993.mq2future;

import java.io.Serializable;

/**
 * mq消息追踪支持
 *
 * @author gcdd1993
 * @date 2021/2/1
 * @since 1.0.0
 */
public interface TraceSupport
        extends Serializable {

    /**
     * 追踪ID
     * <p>
     * req/res都带这个字段
     *
     * @return trace id
     */
    String getTraceId();

    /**
     * 设置追踪ID
     * <p>
     * req/res都带这个字段
     *
     * @param traceId 追踪ID
     */
    void setTraceId(String traceId);

    /**
     * 消息是否超时
     *
     * @return 超时
     */
    boolean isTimeout();

}
