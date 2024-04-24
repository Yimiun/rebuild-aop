package io.yuan.pulsar.handlers.amqp.amqp.service.impl;

import io.vertx.core.impl.ConcurrentHashSet;
import io.yuan.pulsar.handlers.amqp.amqp.component.exchange.Exchange;
import io.yuan.pulsar.handlers.amqp.amqp.component.exchange.PersistentExchange;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.BindData;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.ExchangeData;
import io.yuan.pulsar.handlers.amqp.amqp.service.ExchangeService;
import io.yuan.pulsar.handlers.amqp.configuration.AmqpServiceConfiguration;
import io.yuan.pulsar.handlers.amqp.exception.NotFoundException;
import io.yuan.pulsar.handlers.amqp.exception.ServiceRuntimeException;
import io.yuan.pulsar.handlers.amqp.metadata.MetadataService;
import io.yuan.pulsar.handlers.amqp.utils.FutureExceptionUtils;
import lombok.Getter;
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

    public static final String prefix = "/amqp/exchange/";

    private final MetadataService metadataService;

    private final AmqpServiceConfiguration config;

    @Getter
    private final Map<String, CompletableFuture<Optional<Exchange>>> exchangeMap = new ConcurrentHashMap<>();

    private final Set<String> nonPersistentSet = new ConcurrentHashSet<>();

    public ExchangeServiceImpl(MetadataService metadataService,
                               AmqpServiceConfiguration amqpServiceConfiguration) {
        this.metadataService = metadataService;
        this.config = amqpServiceConfiguration;
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

        String path = generatePath(tenantName, namespaceName, name);
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
            .thenCompose(metadataOps -> {
                if (metadataOps.isEmpty()) {
                    return CompletableFuture.completedFuture(Optional.empty());
                }
                CompletableFuture<Optional<Exchange>> completableFuture = new CompletableFuture<>();
                Exchange exchange = recoveryExchange(metadataOps.get());
                completableFuture.complete(Optional.of(exchange));
                exchangeMap.put(path, completableFuture);
                return completableFuture;
            })
            .whenComplete((__, ex) -> {
                ResourceLockServiceImpl.releaseResourceLock(path);
            });
    }

    @Override
    // @Todo need to handle createIfMissing
    public CompletableFuture<Optional<Exchange>> createExchangeAsync(String name, String tenantName, String namespaceName,
                                                                     String type, boolean durable,
                                                                     boolean autoDelete, boolean internal,
                                                                     Map<String, Object> arguments) {
        return getExchangeAsync(name, tenantName, namespaceName)
            .thenCompose(exchangeOps -> {
                if (exchangeOps.isPresent()) {
                    return CompletableFuture.completedFuture(exchangeOps);
                }

                if (StringUtils.isBlank(name) || StringUtils.isBlank(tenantName) || StringUtils.isBlank(namespaceName)) {
                    log.error("empty arguments when declaring the queue: {}", name);
                    return FutureUtil.failedFuture(new NullPointerException("empty arguments when declaring the queue"));
                }
                ExchangeData data = generateExchangeData(name, tenantName, namespaceName, type, autoDelete, durable, arguments);
                String path = generatePath(tenantName, namespaceName, name);
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

    /**
     *  Call this method and update an exchange's routerMap in metadata
     * */

    @Override
    public CompletableFuture<Void> bind(Exchange exchange, BindData bindData) {
        String path = generatePath(exchange.getTenant(), exchange.getVhost(), exchange.getName());
        return exchange.addBindData(bindData).thenCompose(__ -> {
            return metadataService.modifyUpdateMetadata(ExchangeData.class, path, data -> {
                    return exchange.getExchangeData();
                });
        });
    }

    @Override
    public CompletableFuture<Void> unbind(Exchange exchange, BindData bindData) {
        String path = generatePath(exchange.getTenant(), exchange.getVhost(), exchange.getName());
        return exchange.removeBindData(bindData).thenCompose(__ -> {
            return metadataService.modifyUpdateMetadata(ExchangeData.class, path, data -> {
                return exchange.getExchangeData();
            });
        });
    }

    @Override
    public void close() {
        exchangeMap.clear();
        nonPersistentSet.clear();
    }

    @Override
    public void removeAllExchangesAsync(String tenantName, String namespaceName) {
        String path = generatePath(tenantName, namespaceName);
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

    /**
     *  Call this method and delete an exchange's metadata
     * */

    @Override
    public CompletableFuture<Void> removeExchangeAsync(String name, String tenantName, String namespaceName) {
        String path = generatePath(tenantName, namespaceName, name);
        ResourceLockServiceImpl.acquireResourceLock(path);
        return metadataService.deleteMetadata(path)
            .whenComplete((__, ex) -> {
                ResourceLockServiceImpl.releaseResourceLock(path, true);
                if (ex != null) {
                    log.error("Remove exchange metadata wrong", ex);
                    return;
                }
                log.info("Successfully delete exchange:{}", path);
            });
    }

    private void handleNotification(Notification notification) {
        String path = notification.getPath();
        if (path.startsWith(prefix) && path.split("/").length == 6) {
            if (!exchangeMap.containsKey(path)) {
                if (log.isDebugEnabled()) {
                    log.warn("An exchange request is processed globally, " +
                        "but exchange {} is not initialized on this broker", path.substring(prefix.length()));
                }
                return;
            }
            switch (notification.getType()) {
                case Deleted:
                    handleRemoveExchange(path);
                    log.warn("A Delete request is processed globally, so delete the exchange {} on this broker", path);
                    break;
                case Modified:
                    handleRefreshRouters(path);
                    break;
            }
        } else if (NamespaceResources.pathIsFromNamespace(path)) {
            if (path.split("/").length == 5) {
                NamespaceName namespaceName = NamespaceResources.namespaceFromPath(path);
                switch (notification.getType()) {
                    case Created:
//                        for (String defaultExchange : defaultExchanges) {
//                            createExchangeAsync(defaultExchange, )
//                        }
                        break;
                    case Modified:
                        break;
                    case Deleted:
                        removeAllExchangesAsync(namespaceName.getTenant(), namespaceName.getLocalName());
                        break;
                }
            }
        }

    }

    private void handleRefreshRouters(String pathName) {
        // do something
    }

    // metadata thread run it
    public void handleRemoveExchange(String name) {
        if (log.isDebugEnabled()) {
            log.debug("Exchange:{} has been removed from cache", name);
        }
        CompletableFuture<Optional<Exchange>> removeFuture = exchangeMap.remove(name);
        if (removeFuture != null) {
            if (!removeFuture.isDone() && removeFuture.completeExceptionally(new IllegalStateException())) {
                return;
            }
            removeFuture.thenAccept(ops -> ops.ifPresent(Exchange::close));
        }
        metadataService.invalidPath(ExchangeData.class, name);
    }

    private void generateDefaultExchange() {

    }

    private String generatePath(String... paras) {
        StringJoiner stringJoiner = new StringJoiner("/", prefix, "");
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
