package io.yuan.pulsar.handlers.amqp.amqp.component;

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.service.Topic;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface Exchange {

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

    String getName();

    boolean getDurable();

    boolean getAutoDelete();

    boolean getInternal();

    Map<String, Object> getArguments();

    Exchange.Type getType();

    CompletableFuture<Position> writeToTopic(ByteBuf buf);

    CompletableFuture<Void> close();

    CompletableFuture<Void> start();

    boolean isClosed();

    boolean isStart();
}
