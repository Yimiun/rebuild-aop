package io.yuan.pulsar.handlers.amqp.amqp.service;

import io.yuan.pulsar.handlers.amqp.amqp.component.exchange.Exchange;
import io.yuan.pulsar.handlers.amqp.amqp.component.queue.Queue;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.BindData;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface BindService {

    /**
     * bind queue with an exchange
     *
     * @param source exchangeName. (in dead-letter queue define, it will be reverse)
     * @param destination queueName.    (in dead-letter queue define, it will be reverse)
     * @param type indicate exchange->queue or queue->exchange
     * @param bindingKey bindingKey
     * @param arguments arguments
     *
     * @return void if successful
     * @throws io.yuan.pulsar.handlers.amqp.exception.NotFoundException.ExchangeNotFoundException if exchange not found
     * @throws io.yuan.pulsar.handlers.amqp.exception.NotFoundException.QueueNotFoundException if queue not found
     * @throws org.apache.pulsar.metadata.api.MetadataStoreException if metadata service go wrong
     * */
    CompletableFuture<Void> bind(String tenant, String namespaceName, String source, String destination, String type,
                                 String bindingKey, Map<String, Object> arguments);

    /**
     * unbind queue with an exchange
     *
     * @param exchangeName exchangeName. (in dead-letter queue define, it will be reverse)
     * @param queueName queueName.    (in dead-letter queue define, it will be reverse)
     * @param bindingKey bindingKey
     * @param arguments arguments
     *
     * @return void if successful
     * @throws io.yuan.pulsar.handlers.amqp.exception.NotFoundException.ExchangeNotFoundException if exchange not found
     * @throws io.yuan.pulsar.handlers.amqp.exception.NotFoundException.QueueNotFoundException if queue not found
     * @throws org.apache.pulsar.metadata.api.MetadataStoreException if metadata service go wrong
     * */
    CompletableFuture<Void> unbind(String tenant, String namespaceName, String exchangeName, String queueName,
                                   String bindingKey, Map<String, Object> arguments);

    CompletableFuture<Void> unbindAllFromExchange(Set<BindData> bindDataSet);

    CompletableFuture<Void> unbindAllFromQueue(Set<BindData> bindDataSet);

    CompletableFuture<Void> bindToExchange(Exchange exchange, BindData bindData);

    CompletableFuture<Void> unbindFromExchange(Exchange exchange, BindData bindData);

    CompletableFuture<Void> bindToQueue(Queue queue, BindData newData);

    CompletableFuture<Void> unbindFromQueue(Queue queue, BindData newData);
}
