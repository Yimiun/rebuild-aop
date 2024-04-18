//package io.yuan.pulsar.handlers.amqp.proxy.lookup;
//
//import org.apache.pulsar.common.naming.TopicName;
//
//import java.net.InetSocketAddress;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.CompletableFuture;
//
//public interface AmqpLookupHandler {
//
//    CompletableFuture<InetSocketAddress> findBroker(TopicName topicName);
//
//    CompletableFuture<Map<TopicName,InetSocketAddress>> findBroker(List<TopicName> topicName);
//
//    void close();
//}
