package io.yuan.pulsar.handlers.amqp.amqp.service;

import io.yuan.pulsar.handlers.amqp.amqp.component.Exchange;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface ExchangeService {

    CompletableFuture<Optional<Exchange>> getExchange(String exchangeName, String tenantName, String namespaceName);

    CompletableFuture<Optional<Exchange>> createExchange(String exchangeName, String tenantName, String namespaceName,
                                                     String type, boolean passive, boolean durable,
                                                     boolean autoDelete, boolean internal,
                                                     Map<String, Object> arguments);

    void removeExchange(String exchangeName, String tenantName, String namespaceName);

    CompletableFuture<Void> updateRouter(String exchangeName, String tenantName, String namespaceName,
                                            String queueName, String bindingKey);
}
