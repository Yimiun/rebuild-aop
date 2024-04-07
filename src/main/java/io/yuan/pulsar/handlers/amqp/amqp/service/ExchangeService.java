package io.yuan.pulsar.handlers.amqp.amqp.service;

import io.yuan.pulsar.handlers.amqp.amqp.component.Exchange;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface ExchangeService {

    // 这里除了第一个参数都是默认值参数，也就是如果只给了topic名字，那么按照默认值拼接一个persistent://public/default/topic出来
    CompletableFuture<Optional<Exchange>> getExchange(String name, String tenantName, String namespaceName);

    CompletableFuture<Optional<Exchange>> createExchange(String name, String tenantName, String namespaceName,
                                                     String type, boolean passive, boolean durable,
                                                     boolean autoDelete, boolean internal,
                                                     Map<String, Object> arguments);

    void removeExchange(String name, String tenantName, String namespaceName);
}
