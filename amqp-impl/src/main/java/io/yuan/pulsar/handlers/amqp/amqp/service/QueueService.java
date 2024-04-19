package io.yuan.pulsar.handlers.amqp.amqp.service;

import io.yuan.pulsar.handlers.amqp.amqp.component.queue.Queue;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.BindData;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface QueueService {

    CompletableFuture<Optional<Queue>> getQueue(String queueName, String tenantName, String namespaceName);

    CompletableFuture<Optional<Queue>> createQueue(String queueName, String tenantName, String namespaceName,
                                                   boolean durable, boolean autoDelete, boolean internal,
                                                   boolean exclusive, Map<String, String> arguments,
                                                   List<BindData> bindData, int maxSize, int maxPriority);

    CompletableFuture<Void> updateBindings(List<BindData> newData);

    CompletableFuture<Void> removeQueue(String queueName, String tenantName, String namespaceName);
}
