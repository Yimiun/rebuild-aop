package io.yuan.pulsar.handlers.amqp.amqp.service.impl;

import io.yuan.pulsar.handlers.amqp.amqp.component.exchange.Exchange;
import io.yuan.pulsar.handlers.amqp.amqp.component.queue.Queue;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.BindData;
import io.yuan.pulsar.handlers.amqp.amqp.service.BindService;
import io.yuan.pulsar.handlers.amqp.amqp.service.ExchangeService;
import io.yuan.pulsar.handlers.amqp.amqp.service.QueueService;
import io.yuan.pulsar.handlers.amqp.exception.NotFoundException;
import io.yuan.pulsar.handlers.amqp.exception.ServiceRuntimeException;
import io.yuan.pulsar.handlers.amqp.metadata.MetadataService;
import io.yuan.pulsar.handlers.amqp.utils.FutureExceptionUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.metadata.api.Notification;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class BindServiceImpl implements BindService {

    public static final String QUEUE_TYPE = "QUEUE";

    public static final String EXCHANGE_TYPE = "EXCHANGE";

    private final ExchangeService exchangeService;

    private final QueueService queueService;

    private final MetadataService metadataService;

    public BindServiceImpl(ExchangeService exchangeService, QueueService queueService, MetadataService metadataService) {
        this.exchangeService = exchangeService;
        this.queueService = queueService;
        this.metadataService = metadataService;
        metadataService.registerListener(this::handleNotification);
    }


    @Override
    public CompletableFuture<Void> bind(String tenant, String namespaceName, String source, String destination, String type,
                                        String bindingKey, Map<String, Object> arguments) {
        String exchangeName;
        String queueName;
        if (EXCHANGE_TYPE.equals(type)) {
            exchangeName = source;
            queueName = destination;
        } else {
            exchangeName = destination;
            queueName = source;
        }
        final BindData bindData = new BindData(source, tenant, namespaceName, destination, type, bindingKey, arguments, bindingKey);
        CompletableFuture<Void> bindFuture = new CompletableFuture<>();

        exchangeService.getExchangeAsync(exchangeName, tenant, namespaceName)
            .thenCombine(queueService.getQueue(queueName, tenant, namespaceName), (exchangeOps, queueOps) -> {
                if (queueOps.isEmpty()) {
                    log.error("Error when handling bind request:{}, queue not found", bindData);
                    bindFuture.completeExceptionally(new NotFoundException.QueueNotFoundException());
                    return null;
                } else if (exchangeOps.isEmpty()) {
                    log.error("Error when handling bind request:{}, exchange not found", bindData);
                    bindFuture.completeExceptionally(new NotFoundException.ExchangeNotFoundException());
                    return null;
                }
                Exchange exchange = exchangeOps.get();
                Queue queue = queueOps.get();
                exchangeService.bind(exchange, bindData).thenCombine(queueService.bind(queue, bindData), (e, q) -> {
                    bindFuture.complete(null);
                    return null;
                }).exceptionally(ex -> {
                   Throwable realEx = FutureExceptionUtils.decodeFuture(ex);
                   if (ex instanceof ServiceRuntimeException.DuplicateBindException) {
                       log.warn("Duplicate bind data:{}", bindData);
                       bindFuture.complete(null);
                   } else {
                       log.error("Exception when updating metadata,", realEx);
                       bindFuture.completeExceptionally(realEx);
                   }
                   return null;
                });
                return null;
            }).exceptionally(ex -> {
                log.error("Error when handling bind request:{}, real exception is:", bindData, ex);
                bindFuture.completeExceptionally(ex);
                return null;
            });

        return bindFuture;
    }

    @Override
    public CompletableFuture<Void> unbind(String tenant, String namespaceName, String source, String destination,
                                          String bindingKey, Map<String, Object> arguments) {
//        return FutureUtil.waitForAll();
        return null;
    }

    // process delete exchange -> unbind from queue/ delete queue -> unbind from exchange.
    private void handleNotification(Notification notification) {

    }
}
