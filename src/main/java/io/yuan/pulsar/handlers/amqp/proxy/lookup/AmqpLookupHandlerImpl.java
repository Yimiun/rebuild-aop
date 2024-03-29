package io.yuan.pulsar.handlers.amqp.proxy.lookup;

import lombok.Builder;
import org.apache.pulsar.common.naming.TopicName;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

@Builder
public class AmqpLookupHandlerImpl implements AmqpLookupHandler {

    @Override
    public CompletableFuture<InetSocketAddress> findBroker(TopicName topicName) {
        return null;
    }

    @Override
    public void close() {

    }
}
