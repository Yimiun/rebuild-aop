package io.streamnative.pulsar.handlers.amqp.proxy.lookup;

import org.apache.pulsar.common.naming.TopicName;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

public interface AmqpLookupHandler {

    CompletableFuture<InetSocketAddress> findBroker(TopicName topicName);

    void close();
}
