package io.yuan.pulsar.handlers.amqp.amqp.factory;

import org.apache.pulsar.broker.service.Topic;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface PulsarTopicFactory {

    CompletableFuture<Topic> getOrCreateTopic(String topicName, boolean createIfMissing, Map<String, String> properties);



}
