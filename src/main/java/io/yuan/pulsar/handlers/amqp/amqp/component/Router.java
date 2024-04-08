package io.yuan.pulsar.handlers.amqp.amqp.component;

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.common.naming.TopicName;

import java.util.concurrent.CompletableFuture;

public interface Router {

    /**
     * router:
     * exchange.route(exchange.getName, buf, routingKey)
     *
     * deadQueue.route(exchangeName, buf, routingKey)
     * */
    CompletableFuture<Void> route(TopicName from, ByteBuf buf, String routingKey);

}
