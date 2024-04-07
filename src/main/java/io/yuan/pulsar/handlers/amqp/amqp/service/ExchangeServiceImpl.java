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
import org.apache.bookkeeper.common.util.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ExchangeServiceImpl implements ExchangeService {

    private static final String prefix = "/amqp/exchange/";

    private final MetadataService metadataService;

    private final AmqpServiceConfiguration amqpServiceConfiguration;

    private final Map<String, CompletableFuture<Optional<Exchange>>> exchangeMap = new ConcurrentHashMap<>();

    private final Set<String> nonPersistentSet = new ConcurrentHashSet<>();

    private final Object lock = new Object();

    public ExchangeServiceImpl(MetadataService metadataService,
                               AmqpServiceConfiguration amqpServiceConfiguration) {
        this.metadataService = metadataService;
        this.amqpServiceConfiguration = amqpServiceConfiguration;
        metadataService.registerListener(this::handleNotification);
    }


    @Override
    public CompletableFuture<Optional<Exchange>> getExchange(String name, String tenantName, String namespaceName) {
        return getExchange(name, tenantName, namespaceName, false);
    }

    /**
     * refresh = true:   equals  store.get()
     * refresh = false:  equals  cache.get()
     * */
    public CompletableFuture<Optional<Exchange>> getExchange(String name, String tenantName,
                                                             String namespaceName, boolean refresh) {

        String path = generatePath(tenantName, namespaceName, name);
        if (exchangeMap.containsKey(path)) {
            return exchangeMap.get(name);
        }
        synchronized (lock) {
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
        }
    }

    @Override
    // 如果passive为true，外层应当调用getExchange方法，根据返回值返回信息。
    // 并不更新缓存
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
                        .thenAccept(__ -> {
                            completableFuture.complete(Optional.of(recoveryExchange(name, data)));
                        }).exceptionally(ex -> {
                            Throwable throwable = FutureExceptionUtils.DecodeFuture(ex);
                            if (throwable instanceof MetadataStoreException.AlreadyExistsException) {
                                log.warn("Create Exchange:{} with data:{} failed, Another creation request" +
                                    " accepted by another node has already been created", name, data);
                                // avoid hit old cache
                                getExchange(name, tenantName, namespaceName, true)
                                    .thenAccept(optionalExchange -> {
                                        if (optionalExchange.isPresent()) {
                                            completableFuture.complete(optionalExchange);
                                        } else {
                                            log.error("Create Exchange:{} with data:{} failed, can not create", name, data);
                                            completableFuture.complete(Optional.empty());
                                        }
                                    });
                            } else {
                                log.error("Create Exchange{} with Metadata:{} failed", name, data, ex);
                                completableFuture.complete(Optional.empty());
                            }
                            return null;
                        });
                    return completableFuture;
                }
                return CompletableFuture.completedFuture(exchangeOps);
            });
    }

    private Exchange recoveryExchange(String name, ExchangeData exchangeData) {

        String typeName = exchangeData.getType();
        boolean autoDelete = exchangeData.isAutoDelete();
        boolean internal = exchangeData.isInternal();
        List<BindData> bindData = exchangeData.getBindsData();
        Map<String, Object> args = exchangeData.getArguments();
        Exchange.Type type = Exchange.Type.value(typeName);

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

    @Override
    public void removeExchange(String name, String tenantName, String namespaceName) {
        String path = generatePath(tenantName, namespaceName, name);
        removeExchange(path);
    }

    // 同步，防止出现remove时get的操作，从而出现未close的Exchange
    private void removeExchange(String pathName) {
        synchronized (lock) {
            metadataService.deleteMetadata(ExchangeData.class, pathName)
                .whenComplete((__, ex) -> {
                    if (ex != null) {
                        log.error("Remove exchange metadata wrong", ex);
                    }
                    removeCache(pathName).whenComplete((ops, e) -> {
                        ops.ifPresent(Exchange::close);
                    });
                });
        }
        log.info("Successfully removed exchange:{}", pathName);
    }

    // no need to lock
    public /*synchronized*/ CompletableFuture<Optional<Exchange>> removeCache(String name) {
        if (log.isDebugEnabled()) {
            log.debug("Exchange:{} has been removed from cache", name);
        }
        return exchangeMap.remove(name);
    }

    private String generatePath(String tenant, String namespace, String shortName) {
        return prefix + tenant + "/" + namespace + "/" + shortName;
    }

    private void handleNotification(Notification notification) {
        if (notification.getType().equals(NotificationType.Deleted)) {
            String pathName = notification.getPath().substring(prefix.length());
            removeExchange(pathName);
            log.warn("A Delete request is processed on another broker, so delete the exchange {} on this broker", pathName);
        } else if (notification.getType().equals(NotificationType.Modified)) {
            // change the routing key
            String pathName = notification.getPath().substring(prefix.length());
            metadataService.getTopicMetadata(ExchangeData.class, notification.getPath(), true)
                .thenAccept(ops -> {
                    ops.ifPresent(exchangeData -> refreshRouting(pathName, exchangeData.getBindsData()));
                });
        }
    }

    private void refreshRouting(String name, List<BindData> bindsData) {

    }

}
