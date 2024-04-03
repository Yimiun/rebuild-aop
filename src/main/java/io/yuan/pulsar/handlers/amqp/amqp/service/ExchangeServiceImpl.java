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

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ExchangeServiceImpl implements ExchangeService {

    private static final String prefix = "amqp/exchange";

    private final TopicService topicService;

    private final MetadataService metadataService;

    private final AmqpServiceConfiguration amqpServiceConfiguration;

    private final Map<String, CompletableFuture<Optional<Exchange>>> exchangeMap = new ConcurrentHashMap<>();

    private final Set<String> nonPersistentSet = new ConcurrentHashSet<>();

    public ExchangeServiceImpl(TopicService exchangeService, MetadataService metadataService,
                               AmqpServiceConfiguration amqpServiceConfiguration) {
        this.topicService = exchangeService;
        this.metadataService = metadataService;
        this.amqpServiceConfiguration = amqpServiceConfiguration;
    }

    @Override
    // name是不含任何前缀的topic名 xxxx
    public CompletableFuture<Optional<Exchange>> getExchange(String name, String tenantName,
                                                             String namespaceName, boolean refresh) {

        String path = generatePath(tenantName, namespaceName, name);
        if (exchangeMap.containsKey(path)) {
            // 防止并发情况下出现移除，从而get到null还需要额外判断。
            CompletableFuture<Optional<Exchange>> exchangeFuture = exchangeMap.get(name);
            return exchangeFuture == null ? CompletableFuture.completedFuture(Optional.empty()) : exchangeFuture;
        }

        return metadataService.getTopicMetadata(ExchangeData.class, path, refresh)
                .thenCompose(metadataOps -> {
                    if (metadataOps.isEmpty()) {
                        log.error("Exchange:{} metadata is empty! autoDelete and closed connection", name);
                        removeExchange(name, tenantName, namespaceName)
                            .whenComplete((__, ex) -> {
                                log.warn("Caused by exchange metadata empty, exchange:{} autoDelete done", name);
                            });
                        return CompletableFuture.completedFuture(Optional.empty());
                    }
                    CompletableFuture<Optional<Exchange>> completableFuture = new CompletableFuture<>();
                    Exchange exchange = recoveryExchange(name, metadataOps.get());
                    completableFuture.complete(Optional.of(exchange));
                    exchangeMap.put(path, completableFuture);
                    return completableFuture;
                });
        }

    @Override
    // 如果passive为true，外层应当调用getExchange方法，根据返回值返回信息。
    // 由于是用的cache的方法，忽略掉了zk返回的版本信息，需要在
    public CompletableFuture<Optional<Exchange>> createExchange(String name, String tenantName, String namespaceName,
                                                                String type, boolean passive, boolean durable,
                                                                boolean autoDelete, boolean internal,
                                                                Map<String, Object> arguments) {
        return getExchange(name, tenantName, namespaceName, false)
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

    @Override
    public CompletableFuture<Void> removeExchange(String name, String tenantName, String namespaceName) {
        removeCache(name);
        return null;
    }

    private Exchange recoveryExchange(String name, ExchangeData exchangeData) {

        String typeName = exchangeData.getType();
        boolean autoDelete = exchangeData.isAutoDelete();
        boolean internal = exchangeData.isInternal();
        BindData bindData = exchangeData.getBindData();
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
        exchangeData.setBindData(new BindData());
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

    // no need lock
    public /*synchronized*/ void removeCache(String name) {
        exchangeMap.remove(name).thenAccept(ops -> {
            ops.ifPresent(Exchange::close);
        });
    }

    private String generatePath(String tenant, String namespace, String shortName) {
        return prefix + "/" + tenant + "/" + namespace + "/" + shortName;
    }

//    private boolean checkEquals(Exchange exchange, ExchangeData exchangeData) {
//        return exchange.getAutoDelete() == exchangeData.isAutoDelete()
//            && exchange.getDurable() == exchangeData.isDurable()
//            && exchange.getArguments().equals(exchange.getArguments())
//            && exchange.getName().equals(exchangeData.getName())
//            && exchange.getType().name().equals(exchangeData.getType())
//            && exchange.
//    }

}
