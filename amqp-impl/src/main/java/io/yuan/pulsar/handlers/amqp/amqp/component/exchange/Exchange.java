package io.yuan.pulsar.handlers.amqp.amqp.component.exchange;

import io.yuan.pulsar.handlers.amqp.amqp.component.Router;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.BindData;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.ExchangeData;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface Exchange extends Router {

    enum Type{
        Direct,
        Fanout,
        Topic,
        Headers;

        public static Type value(String type) {
            if (type == null || type.length() == 0) {
                return null;
            }
            type = type.toLowerCase();
            switch (type) {
                case "direct":
                    return Direct;
                case "fanout":
                    return Fanout;
                case "topic":
                    return Topic;
                case "headers":
                    return Headers;
                default:
                    return null;
            }
        }
    }

    CompletableFuture<Void> addBindData(BindData bindData);

    CompletableFuture<Void> removeBindData(BindData bindData);

    Set<BindData> getBindData();

    String getTenant();

    String getVhost();

    String getName();

    boolean getDurable();

    boolean getAutoDelete();

    boolean getInternal();

    Map<String, Object> getArguments();

    Exchange.Type getType();

    CompletableFuture<Void> close();

    ExchangeData getExchangeData();

    void start();

    boolean isClosed();

    boolean isStart();
}
