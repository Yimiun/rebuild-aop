package io.yuan.pulsar.handlers.amqp.amqp.service;

import io.vertx.core.impl.ConcurrentHashSet;
import io.yuan.pulsar.handlers.amqp.amqp.binding.BindData;
import io.yuan.pulsar.handlers.amqp.amqp.binding.ExchangeData;
import io.yuan.pulsar.handlers.amqp.amqp.component.Exchange;
import io.yuan.pulsar.handlers.amqp.amqp.component.PersistentExchange;
import io.yuan.pulsar.handlers.amqp.configuration.AmqpServiceConfiguration;
import io.yuan.pulsar.handlers.amqp.metadata.MetadataService;
import io.yuan.pulsar.handlers.amqp.utils.FutureExceptionUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class ExchangeServiceImpl implements ExchangeService {

    private static final String prefix = "/amqp/exchange/";

    private final MetadataService metadataService;

    private final AmqpServiceConfiguration config;

    private final Map<String, CompletableFuture<Optional<Exchange>>> exchangeMap = new ConcurrentHashMap<>();

    private final Set<String> nonPersistentSet = new ConcurrentHashSet<>();

    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public ExchangeServiceImpl(MetadataService metadataService,
                               AmqpServiceConfiguration amqpServiceConfiguration) {
        this.metadataService = metadataService;
        this.config = amqpServiceConfiguration;
        metadataService.registerListener(this::handleNotification);
    }


    @Override
    public CompletableFuture<Optional<Exchange>> getExchange(String name, String tenantName, String namespaceName) {
        return getExchange(name, tenantName, namespaceName, false);
    }

    /**
     * refresh = true:   equals  store.get()
     * refresh = false:  equals  cache.get()
     * ensure exchange singleton
     * ensure multi threads safety
     * @Todo authorization
     * */
    public CompletableFuture<Optional<Exchange>> getExchange(String name, String tenantName,
                                                             String namespaceName, boolean refresh) {

        String path = generatePath(tenantName, namespaceName, name);
        if (exchangeMap.containsKey(path)) {
            CompletableFuture<Optional<Exchange>> res = exchangeMap.get(path);
            return res == null ? CompletableFuture.completedFuture(Optional.empty()) : res;
        }
        // to avoid delete-get multi threads operation going wrong, which causes exchange stats wrong
        readWriteLock.readLock().lock();
        try{
            // singleton, one exchange only has one instance
            if (exchangeMap.containsKey(path)) {
                return exchangeMap.get(path);
            }
            return metadataService.getTopicMetadata(ExchangeData.class, path, refresh)
                .thenCompose(metadataOps -> {
                    if (metadataOps.isEmpty()) {
                        log.error("Exchange:{} metadata is empty! create it first", name);
                        return CompletableFuture.completedFuture(Optional.empty());
                    }
                    CompletableFuture<Optional<Exchange>> completableFuture = new CompletableFuture<>();
                    Exchange exchange = recoveryExchange(name, metadataOps.get());
                    completableFuture.complete(Optional.of(exchange));
                    exchangeMap.put(path, completableFuture);
                    return completableFuture;
                });
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    // All cache generation operations are performed through the get method, and for this non-high-frequency operation,
    // some performance can be sacrificed for high security and singleton performance
    public CompletableFuture<Optional<Exchange>> createExchange(String name, String tenantName, String namespaceName,
                                                                String type, boolean passive, boolean durable,
                                                                boolean autoDelete, boolean internal,
                                                                Map<String, Object> arguments) {
        return getExchange(name, tenantName, namespaceName)
            .thenCompose(exchangeOps -> {
                if (exchangeOps.isEmpty()) {
                    if (StringUtils.isBlank(name) || StringUtils.isBlank(tenantName) || StringUtils.isBlank(namespaceName)) {
                        log.error("empty arguments when declare the exchange: {}", name);
                        return CompletableFuture.completedFuture(Optional.empty());
                    }
                    ExchangeData data = generateExchangeData(name, namespaceName, type, autoDelete, durable, arguments);
                    String path = generatePath(tenantName, namespaceName, name);
                    final CompletableFuture<Optional<Exchange>> completableFuture = new CompletableFuture<>();
                    metadataService.updateTopicMetadata(ExchangeData.class, data, path, false)
                        .whenComplete((__, ex) -> {
                            if (ex == null || FutureExceptionUtils.DecodeFuture(ex) instanceof
                                    MetadataStoreException.AlreadyExistsException) {

                                if (ex != null) {
                                    log.warn("Create Exchange:{} with data:{} failed, Another creation request" +
                                        " accepted by another node has already been created", name, data);
                                }
                                getExchange(name, tenantName, namespaceName, true)
                                    .thenAccept(optionalExchange -> {
                                        if (optionalExchange.isPresent()) {
                                            completableFuture.complete(optionalExchange);
                                        } else {
                                            log.error("Create Exchange:{} with data:{} failed, can not create", name, data);
                                            completableFuture.complete(Optional.empty());
                                        }
                                    });
                                return;
                            }
                            log.error("Create Exchange{} with Metadata:{} failed", name, data, ex);
                            completableFuture.complete(Optional.empty());
                        });
                }
                return CompletableFuture.completedFuture(exchangeOps);
            });
    }

    @Override
    /**
     *
     * */
    public CompletableFuture<Void> updateRouter(String exchangeName, String tenantName, String namespaceName,
                                                    String queueName, String bindingKey) {

        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        getExchange(exchangeName, tenantName, namespaceName)
            .thenCompose(ops -> {
                if (ops.isEmpty()) {
                    log.error("There is no exchange named:{}, so update routers failed", exchangeName);
                    completableFuture.completeExceptionally(new NotFoundException.ExchangeNotFoundException());
                    return null;
                }
                List<BindData> bindData = ops.get().getBindData();
                return null;
            });

        return completableFuture;
    }

    @Override
    // delete method
    public void removeExchange(String name, String tenantName, String namespaceName) {
        String path = generatePath(tenantName, namespaceName, name);
        readWriteLock.writeLock().lock();
        try {
            metadataService.deleteMetadata(ExchangeData.class, path)
                .whenComplete((__, ex) -> {
                    if (ex != null) {
                        log.error("Remove exchange metadata wrong", ex);
                        // maybe do something???????
                    }
                });
        } finally {
            readWriteLock.writeLock().unlock();
        }
        log.info("Successfully delete exchange:{}", path);
    }

    private void handleNotification(Notification notification) {
        if (notification.getType().equals(NotificationType.Deleted)) {
            String pathName = notification.getPath().substring(prefix.length());
            removeCache(pathName);
            log.warn("A Delete request is processed globally, so delete the exchange {} on this broker", pathName);
        } else if (notification.getType().equals(NotificationType.Modified)) {
            // change the routing key
            String pathName = notification.getPath().substring(prefix.length());
            metadataService.getTopicMetadata(ExchangeData.class, notification.getPath(), true)
                .thenAccept(ops -> {
                    ops.ifPresent(exchangeData -> {
//                        log.info();
                        refreshRouting(pathName, exchangeData.getBindsData());
                    });
                });
        }
    }

    private void refreshRouting(String pathName, List<BindData> bindsData) {

    }

    public void removeCache(String name) {
        if (log.isDebugEnabled()) {
            log.debug("Exchange:{} has been removed from cache", name);
        }
        exchangeMap.remove(name);
    }

    private String generatePath(String tenant, String namespace, String shortName) {
        return prefix + tenant + "/" + namespace + "/" + shortName;
    }

    private Exchange recoveryExchange(String name, ExchangeData exchangeData) {

        String typeName = exchangeData.getType();
        boolean autoDelete = exchangeData.isAutoDelete();
        boolean internal = exchangeData.isInternal();
        List<BindData> bindData = exchangeData.getBindsData();
        Map<String, Object> args = exchangeData.getArguments();
        Exchange.Type type = Exchange.Type.value(typeName);

        // non-persistent
        return new PersistentExchange(name, type, true, autoDelete, internal, bindData, args);
    }

    private ExchangeData generateExchangeData(String name, String vhost, String type,
                                              boolean autoDelete, boolean durable, Map<String, Object> arguments) {
        ExchangeData exchangeData = new ExchangeData();
        exchangeData.setName(name);
        exchangeData.setVhost(vhost);
        exchangeData.setDurable(durable);
        exchangeData.setBindsData(new ArrayList<>());
        if (StringUtils.isEmpty(type)) {
            exchangeData.setType(Exchange.Type.Direct.name());
        } else {
            exchangeData.setType(type);
        }
        exchangeData.setInternal(false);
        exchangeData.setArguments(arguments);
        exchangeData.setAutoDelete(autoDelete);
        return exchangeData;
    }

}
