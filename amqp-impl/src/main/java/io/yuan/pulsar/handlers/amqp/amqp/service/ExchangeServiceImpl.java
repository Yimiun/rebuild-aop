package io.yuan.pulsar.handlers.amqp.amqp.service;

import io.vertx.core.impl.ConcurrentHashSet;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.BindData;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.ExchangeData;
import io.yuan.pulsar.handlers.amqp.amqp.component.Exchange;
import io.yuan.pulsar.handlers.amqp.amqp.component.PersistentExchange;
import io.yuan.pulsar.handlers.amqp.configuration.AmqpServiceConfiguration;
import io.yuan.pulsar.handlers.amqp.metadata.MetadataService;
import io.yuan.pulsar.handlers.amqp.utils.FutureExceptionUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.common.naming.NamespaceName;
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

    private final Object lock = new Object();

    public ExchangeServiceImpl(MetadataService metadataService,
                               AmqpServiceConfiguration amqpServiceConfiguration) {
        this.metadataService = metadataService;
        this.config = amqpServiceConfiguration;
        metadataService.registerListener(this::handleNotification);
    }


    @Override
    public CompletableFuture<Optional<Exchange>> getExchange(String name, String tenantName, String namespaceName) {
        return getExchange(name, tenantName, namespaceName, false, false);
    }

    @Override
    public CompletableFuture<Optional<Exchange>> queryExchange(String name, String tenantName, String namespaceName) {
        return getExchange(name, tenantName, namespaceName, false, true);
    }

    /**
     * refresh = true:   equals  store.get()
     * refresh = false:  equals  cache.get()
     * ensure exchange singleton
     * ensure multi threads safety
     *
     *    return val :
     *    if (empty) closeConnection
     * @Todo authorization
     * */
    public CompletableFuture<Optional<Exchange>> getExchange(String name, String tenantName,
                                                             String namespaceName, boolean refresh, boolean query) {

        String path = generatePath(tenantName, namespaceName, name);
        if (exchangeMap.containsKey(path)) {
            CompletableFuture<Optional<Exchange>> res = exchangeMap.get(path);
            return res == null ? CompletableFuture.completedFuture(Optional.empty()) : res;
        }
        // to avoid delete-get metadata multi threads operation going wrong, which causes exchange stats wrong
        synchronized (lock) {
            // singleton, one exchange only has one instance
            if (exchangeMap.containsKey(path)) {
                return exchangeMap.get(path);
            }
            return metadataService.getMetadata(ExchangeData.class, path, refresh)
                .thenCompose(metadataOps -> {
                    if (metadataOps.isEmpty()) {
                        log.warn("Exchange:{} metadata is empty!", name);
                        return CompletableFuture.completedFuture(Optional.empty());
                    }
                    CompletableFuture<Optional<Exchange>> completableFuture = new CompletableFuture<>();
                    Exchange exchange = recoveryExchange(metadataOps.get());
                    completableFuture.complete(Optional.of(exchange));
                    if (!query) {
                        exchangeMap.put(path, completableFuture);
                    }
                    return completableFuture;
                });
        }
    }

    /**
     * All exchangeMap-cached operations are performed through the GET-method, and for this non-high-frequency operation
     * some performance can be sacrificed for strong consistency and singleton performance
     * The reason for not using Request-then-Handle way is to quickly generate new instances
     *
     *      return val :
     *      if (empty) closeConnection
     * */

    @Override
    // @Todo need to handle createIfMissing
    public CompletableFuture<Optional<Exchange>> createExchange(String name, String tenantName, String namespaceName,
                                                                String type, boolean durable,
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
                    metadataService.updateMetadata(ExchangeData.class, data, path, false)
                        .whenComplete((__, ex) -> {
                            if (ex == null || FutureExceptionUtils.DecodeFuture(ex) instanceof
                                    MetadataStoreException.AlreadyExistsException) {
                                if (ex != null) {
                                    log.warn("Create Exchange:{} with data:{} failed, Another creation request" +
                                        " accepted by another node has already been created", name, data);
                                }
                                getExchange(name, tenantName, namespaceName, true, false)
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
                    return completableFuture;
                }
                return CompletableFuture.completedFuture(exchangeOps);
            });
    }

    /**
     *  Call this method and update an exchange's routerMap in metadata
     * */

    @Override
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
    public void close() {
        synchronized (lock) {
            exchangeMap.clear();
            nonPersistentSet.clear();
        }
    }

    @Override
    public void removeAllExchanges(String tenantName, String namespaceName) {
        String path = generatePath(tenantName, namespaceName);
        synchronized (lock) {
            metadataService.deleteMetadataRecursive(path)
                .whenComplete((__, ex) -> {
                    if (ex != null) {
                        log.error("Remove exchange metadata wrong", ex);
                        return;
                    }
                    log.info("Successfully removed namespace path:{}", path);
                });
        }
    }

    /**
     *  Call this method and delete an exchange's metadata
     * */

    @Override
    public CompletableFuture<Void> removeExchange(String name, String tenantName, String namespaceName) {
        String path = generatePath(tenantName, namespaceName, name);
        synchronized (lock) {
            return metadataService.deleteMetadata(path)
                .whenComplete((__, ex) -> {
                    if (ex != null) {
                        log.error("Remove exchange metadata wrong", ex);
                        return;
                    }
                    log.info("Successfully delete exchange:{}", path);
                });
        }
    }

    private void handleNotification(Notification notification) {
        String path = notification.getPath();
        if (path.startsWith(prefix) && path.split("/").length == 6) {
            String pathName = path.substring(prefix.length());
            if (!exchangeMap.containsKey(pathName)) {
                if (log.isDebugEnabled()) {
                    log.warn("An exchange request is processed globally, " +
                        "but exchange {} is not initialized on this broker", pathName);
                }
                return;
            }
            switch (notification.getType()) {
                case Deleted:
                    handleRemoveExchange(pathName);
                    log.warn("A Delete request is processed globally, so delete the exchange {} on this broker", pathName);
                    break;
                case Modified:
                    metadataService.getMetadata(ExchangeData.class, path, true)
                        .thenAccept(ops -> {
                            ops.ifPresent(exchangeData -> {
                                handleRefreshRouters(pathName, exchangeData.getBindsData());
                            });
                        });
                    break;
            }
        } else if (NamespaceResources.pathIsFromNamespace(path)) {
            if (path.split("/").length == 5) {
                NamespaceName namespaceName = NamespaceResources.namespaceFromPath(path);
                switch (notification.getType()) {
                    case Created:
//                        for (String defaultExchange : defaultExchanges) {
//                            createExchange(defaultExchange, )
//                        }
                        break;
                    case Modified:
                        break;
                    case Deleted:
                        removeAllExchanges(namespaceName.getTenant(), namespaceName.getLocalName());
                        break;
                }
            }
        }

    }

    private void handleRefreshRouters(String pathName, List<BindData> bindsData) {
        // do something
    }

    public void handleRemoveExchange(String name) {
        if (log.isDebugEnabled()) {
            log.debug("Exchange:{} has been removed from cache", name);
        }
        CompletableFuture<Optional<Exchange>> removeFuture = exchangeMap.remove(name);
        if (removeFuture != null) {
            // must present
            removeFuture.thenAccept(ops -> ops.get().close());
        }
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

    private Exchange recoveryExchange(ExchangeData exchangeData) {
        String name = exchangeData.getName();
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
