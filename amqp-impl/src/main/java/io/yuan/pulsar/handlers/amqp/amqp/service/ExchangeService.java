package io.yuan.pulsar.handlers.amqp.amqp.service;

import io.yuan.pulsar.handlers.amqp.amqp.component.exchange.Exchange;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface ExchangeService {

    CompletableFuture<Optional<Exchange>> getExchange(String exchangeName, String tenantName, String namespaceName);

    CompletableFuture<Optional<Exchange>> queryExchange(String exchangeName, String tenantName, String namespaceName);

    CompletableFuture<Optional<Exchange>> createExchange(String exchangeName, String tenantName, String namespaceName,
                                                     String type, boolean durable,
                                                     boolean autoDelete, boolean internal,
                                                     Map<String, Object> arguments);

    CompletableFuture<Void> removeExchange(String exchangeName, String tenantName, String namespaceName);

    CompletableFuture<Void> updateRouter(String exchangeName, String tenantName, String namespaceName,
                                            String queueName, String bindingKey);

    void close();

    void removeAllExchanges(String tenantName, String namespaceName);

}
