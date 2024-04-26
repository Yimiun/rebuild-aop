package io.yuan.pulsar.handlers.amqp.amqp.service.impl;

import io.vertx.core.impl.ConcurrentHashSet;
import io.yuan.pulsar.handlers.amqp.amqp.component.exchange.Exchange;
import io.yuan.pulsar.handlers.amqp.amqp.component.exchange.PersistentExchange;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.ExchangeData;
import io.yuan.pulsar.handlers.amqp.amqp.service.BindService;
import io.yuan.pulsar.handlers.amqp.amqp.service.ExchangeService;
import io.yuan.pulsar.handlers.amqp.broker.AmqpBrokerService;
import io.yuan.pulsar.handlers.amqp.configuration.AmqpServiceConfiguration;
import io.yuan.pulsar.handlers.amqp.exception.NotFoundException;
import io.yuan.pulsar.handlers.amqp.exception.ServiceRuntimeException;
import io.yuan.pulsar.handlers.amqp.metadata.MetadataService;
import io.yuan.pulsar.handlers.amqp.utils.FutureExceptionUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ExchangeServiceImpl implements ExchangeService {

    private final String[] defaultExchanges =
            new String[] {"amq.direct","amq.fanout","amq.headers","amq.match","amq.topic"};

    public static final String EXCHANGE_PREFIX = "/amqp/exchange/";

    private final MetadataService metadataService;

    private final BindService bindService;

    private final AmqpServiceConfiguration config;

    // all the operation of it must be serialized! or will dirty-reading, use ResourceLock
    private final Map<String, CompletableFuture<Optional<Exchange>>> exchangeMap = new ConcurrentHashMap<>();

    private final Set<String> nonPersistentSet = new ConcurrentHashSet<>();

    public ExchangeServiceImpl(AmqpBrokerService brokerService) {
        this.metadataService = brokerService.getMetadataService();
        this.bindService = brokerService.getBindService();
        this.config = brokerService.getAmqpServiceConfiguration();
        metadataService.registerListener(this::handleNotification);
    }


    @Override
    public CompletableFuture<Optional<Exchange>> getExchangeAsync(String name, String tenantName, String namespaceName) {
        return getExchange(name, tenantName, namespaceName, true);
    }

    /**
     * refresh = true:   equals  store.get()
     * refresh = false:  equals  cache.get()
     * ensure exchange singleton
     *
     *    return val :
     *    if (empty) closeConnection
     * @Todo authorization
     * */
    public CompletableFuture<Optional<Exchange>> getExchange(String name, String tenantName,
                                                             String namespaceName, boolean refresh) {

        String path = generateExchangePath(tenantName, namespaceName, name);
        if (exchangeMap.containsKey(path)) {
            CompletableFuture<Optional<Exchange>> res = exchangeMap.get(path);
            return res != null ? res : FutureUtil.failedFuture(new NotFoundException.ExchangeNotFoundException());
        }
        // avoid multi get
        ResourceLockServiceImpl.acquireResourceLock(path);
        if (exchangeMap.containsKey(path)) {
            ResourceLockServiceImpl.releaseResourceLock(path);
            return exchangeMap.get(path);
        }
        return metadataService.getMetadata(ExchangeData.class, path, refresh)
            .thenCompose(metadataOpt -> {
                if (metadataOpt.isEmpty()) {
                    return CompletableFuture.completedFuture(Optional.empty());
                }
                CompletableFuture<Optional<Exchange>> completableFuture = new CompletableFuture<>();
                Exchange exchange = recoveryExchange(metadataOpt.get());
                completableFuture.complete(Optional.of(exchange));
                exchangeMap.put(path, completableFuture);
                return completableFuture;
            })
            .whenComplete((__, ex) -> {
                ResourceLockServiceImpl.releaseResourceLock(path);
            });
    }

    @Override
    // @todo need to handle createIfMissing
    public CompletableFuture<Optional<Exchange>> createExchangeAsync(String name, String tenantName, String namespaceName,
                                                                     String type, boolean durable,
                                                                     boolean autoDelete, boolean internal,
                                                                     Map<String, Object> arguments) {
        return getExchangeAsync(name, tenantName, namespaceName)
            .thenCompose(exchangeOpt -> {
                if (exchangeOpt.isPresent()) {
                    return CompletableFuture.completedFuture(exchangeOpt);
                }

                if (StringUtils.isBlank(name) || StringUtils.isBlank(tenantName) || StringUtils.isBlank(namespaceName)) {
                    log.error("empty arguments when declaring the queue: {}", name);
                    return FutureUtil.failedFuture(new NullPointerException("empty arguments when declaring the queue"));
                }
                ExchangeData data = generateExchangeData(name, tenantName, namespaceName, type, autoDelete, durable, arguments);
                String path = generateExchangePath(tenantName, namespaceName, name);
                return metadataService.createMetadata(ExchangeData.class, data, path, false)
                    .exceptionally(ex -> {
                        Throwable e = FutureExceptionUtils.decodeFuture(ex);
                        if (e instanceof MetadataStoreException.AlreadyExistsException) {
                            log.warn("Create Exchange:{} with data:{} failed, another creation request" +
                                " accepted by another node has already been created", name, data);
                            return null;
                        } else {
                            throw (RuntimeException) e;
                        }
                    })
                    .thenCompose(__ -> {
                        log.info("Successfully create exchange :{}", data);
                        return getExchangeAsync(name, tenantName, namespaceName);
                    });
            });
    }

    @Override
    public void close() {
        exchangeMap.clear();
        nonPersistentSet.clear();
    }

    /**
     *  Call this method and delete an exchange's metadata
     * */

    @Override
    public CompletableFuture<Void> removeExchangeAsync(String name, String tenantName, String namespaceName) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        String path = generateExchangePath(tenantName, namespaceName, name);
        getExchange(name, tenantName, namespaceName, true)
            .whenComplete((exchangeOpt, ex) ->{
                if (ex != null) {
                    future.completeExceptionally(ex.getCause());
                    return;
                }
                if (exchangeOpt.isEmpty()) {
                    future.completeExceptionally(new NotFoundException.ExchangeNotFoundException("Exchange not found"));
                    return;
                }
                bindService.unbindAllFromQueue(exchangeOpt.get().getBindData())
                    .whenComplete((ignore1, unbindEx) -> {
                        if (unbindEx != null) {
                            future.completeExceptionally(unbindEx.getCause());
                            return;
                        }
                        ResourceLockServiceImpl.acquireResourceLock(path);
                        metadataService.deleteMetadata(path)
                            .whenComplete((ignore2, deleteEx) -> {
                                ResourceLockServiceImpl.releaseResourceLock(path, true);
                                if (deleteEx != null) {
                                    log.error("Remove exchange metadata wrong", deleteEx);
                                    future.completeExceptionally(deleteEx.getCause());
                                    return;
                                }
                                log.info("Successfully delete exchange:{}", path);
                                future.complete(null);
                            });
                    });
            });
        return future;
    }

    private void handleNotification(Notification notification) {
        String path = notification.getPath();
        if (path.startsWith(EXCHANGE_PREFIX) && path.split("/").length == 6) {
            if (!exchangeMap.containsKey(path)) {
                if (log.isDebugEnabled()) {
                    log.warn("An exchange request is processed globally, " +
                        "but exchange {} is not initialized on this broker", path.substring(EXCHANGE_PREFIX.length()));
                }
                return;
            }
            switch (notification.getType()) {
                case Deleted:
                    handleRemoveExchange(path);
                    log.warn("A Delete request is processed globally, so delete the exchange {} on this broker",
                        path.substring(EXCHANGE_PREFIX.length()));
                    break;
                case Modified:
                    break;
            }
        } else if (NamespaceResources.pathIsFromNamespace(path)) {
            if (path.split("/").length == 5) {
                NamespaceName namespaceName = NamespaceResources.namespaceFromPath(path);
                switch (notification.getType()) {
                    case Created:
                    case Modified:
                        break;
                    case Deleted:
                        handleRemoveAllExchanges(namespaceName.getTenant(), namespaceName.getLocalName());
                        break;
                }
            }
        }

    }

    // metadata thread run it
    public void handleRemoveExchange(String path) {
        if (log.isDebugEnabled()) {
            log.debug("Exchange:{} has been removed from cache", path);
        }
        CompletableFuture<Optional<Exchange>> removeFuture = exchangeMap.remove(path);
        if (removeFuture != null) {
            removeFuture.thenAccept(opt -> opt.ifPresent(Exchange::close));
        }
        metadataService.invalidPath(ExchangeData.class, path);
    }

    private void handleRemoveAllExchanges(String tenantName, String namespaceName) {
        String path = generateExchangePath(tenantName, namespaceName);
        ResourceLockServiceImpl.acquireResourceLock(path);
        metadataService.deleteMetadataRecursive(path)
            .whenComplete((__, ex) -> {
                ResourceLockServiceImpl.releaseResourceLock(path, true);
                if (ex != null) {
                    log.error("Remove exchange metadata wrong", ex);
                    return;
                }
                log.info("Successfully removed namespace path:{}", path);
            });
    }

    public static String generateExchangePath(String... paras) {
        StringJoiner stringJoiner = new StringJoiner("/", EXCHANGE_PREFIX, "");
        for(String para : paras) {
            stringJoiner.add(para);
        }
        return stringJoiner.toString();
    }

    //add check
    private Exchange recoveryExchange(ExchangeData exchangeData) throws RuntimeException{
        String name = exchangeData.getName();
        if (StringUtils.isEmpty(name)) {
            throw new ServiceRuntimeException.ExchangeParameterException("Exchange name is empty");
        }
        String typeName = exchangeData.getType();
        if (StringUtils.isEmpty(typeName)) {
            throw new ServiceRuntimeException.ExchangeParameterException("Exchange typeName is empty");
        }

        // non-persistent-Exchange
        return new PersistentExchange(exchangeData);
    }

    private ExchangeData generateExchangeData(String name, String tenant, String vhost, String type,
                                              boolean autoDelete, boolean durable, Map<String, Object> arguments) {
        ExchangeData exchangeData = new ExchangeData();
        exchangeData.setName(name);
        exchangeData.setTenant(tenant);
        exchangeData.setVhost(vhost);
        exchangeData.setDurable(durable);
        exchangeData.setBindsData(new HashSet<>());
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
