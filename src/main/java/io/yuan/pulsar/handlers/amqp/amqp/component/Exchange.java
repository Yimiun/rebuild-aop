package io.yuan.pulsar.handlers.amqp.amqp.component;

import io.yuan.pulsar.handlers.amqp.amqp.binding.BindData;
import java.util.List;
import java.util.Map;
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

    List<BindData> getBindData();

    String getName();

    boolean getDurable();

    boolean getAutoDelete();

    boolean getInternal();

    Map<String, Object> getArguments();

    Exchange.Type getType();

    CompletableFuture<Void> close();

    void start();

    boolean isClosed();

    boolean isStart();
}
