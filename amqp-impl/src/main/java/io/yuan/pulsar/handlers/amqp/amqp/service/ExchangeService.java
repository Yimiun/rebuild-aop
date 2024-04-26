package io.yuan.pulsar.handlers.amqp.amqp.service;

import io.yuan.pulsar.handlers.amqp.amqp.component.exchange.Exchange;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.BindData;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface ExchangeService {

    CompletableFuture<Optional<Exchange>> getExchangeAsync(String exchangeName, String tenantName, String namespaceName);

    /**
     * All exchangeMap-cached operations are performed through the GET-method, and for this non-high-frequency operation
     * some performance can be sacrificed for strong consistency and singleton performance
     *
     * @param exchangeName: short name of queue.
     * @param tenantName: tenant name.
     * @param type: type of exchange, string format
     * @param durable: persistent or non-persistent, true if we want a persistent queue
     * @param autoDelete: true if the server should delete the queue when it is no longer in use
     * @param internal: true if
     * */
    CompletableFuture<Optional<Exchange>> createExchangeAsync(String exchangeName, String tenantName, String namespaceName,
                                                              String type, boolean durable,
                                                              boolean autoDelete, boolean internal,
                                                              Map<String, Object> arguments);

    CompletableFuture<Void> removeExchangeAsync(String exchangeName, String tenantName, String namespaceName);

    void close();

}
