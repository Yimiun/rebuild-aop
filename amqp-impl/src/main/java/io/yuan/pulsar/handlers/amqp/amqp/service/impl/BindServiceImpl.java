package io.yuan.pulsar.handlers.amqp.amqp.service.impl;

import io.yuan.pulsar.handlers.amqp.amqp.component.exchange.Exchange;
import io.yuan.pulsar.handlers.amqp.amqp.component.queue.Queue;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.BindData;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.ExchangeData;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.QueueData;
import io.yuan.pulsar.handlers.amqp.amqp.service.BindService;
import io.yuan.pulsar.handlers.amqp.amqp.service.ExchangeService;
import io.yuan.pulsar.handlers.amqp.amqp.service.QueueService;
import io.yuan.pulsar.handlers.amqp.broker.AmqpBrokerService;
import io.yuan.pulsar.handlers.amqp.exception.NotFoundException;
import io.yuan.pulsar.handlers.amqp.exception.ServiceRuntimeException;
import io.yuan.pulsar.handlers.amqp.metadata.MetadataService;
import io.yuan.pulsar.handlers.amqp.utils.FutureExceptionUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeBoundOkBody;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * First modify the memory, then update the metadata.
 * When modifying the same data with high concurrency in multiple threads, memory consistency can be ensured.
 * However, metadata consistency requires network trustworthiness,
 * which meaning that the metadata modification request sent first must reach the zookeeper first
 * */
@Slf4j
public class BindServiceImpl implements BindService {

    public static final String QUEUE_TYPE = "QUEUE";

    public static final String EXCHANGE_TYPE = "EXCHANGE";

    private final AmqpBrokerService brokerService;

    private final ExchangeService exchangeService;

    private final QueueService queueService;

    private final MetadataService metadataService;

    public BindServiceImpl(AmqpBrokerService brokerService) {
        this.brokerService = brokerService;
        this.exchangeService = brokerService.getExchangeService();
        this.queueService = brokerService.getQueueService();
        this.metadataService = brokerService.getMetadataService();
//        metadataService.registerListener(this::handleNotification);
    }


    @Override
    public CompletableFuture<Void> bind(String tenant, String namespace, String source, String destination, String type,
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
        final BindData bindData = new BindData(source, tenant, namespace, destination,
                type, bindingKey, arguments, bindingKey);

        CompletableFuture<Void> bindFuture = new CompletableFuture<>();
        exchangeService.getExchangeAsync(exchangeName, tenant, namespace)
            .thenAcceptBoth(queueService.getQueue(queueName, tenant, namespace), (exchangeOps, queueOps) -> {
                if (queueOps.isEmpty()) {
                    log.error("Error when handling bind request:{}, queue not found", bindData);
                    bindFuture.completeExceptionally(new NotFoundException.QueueNotFoundException());
                    return;
                } else if (exchangeOps.isEmpty()) {
                    log.error("Error when handling bind request:{}, exchange not found", bindData);
                    bindFuture.completeExceptionally(new NotFoundException.ExchangeNotFoundException());
                    return;
                }
                Exchange exchange = exchangeOps.get();
                Queue queue = queueOps.get();
                bindToExchange(exchange, bindData).thenAcceptBoth(bindToQueue(queue, bindData), (e, q) -> {
                    bindFuture.complete(null);
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
            }).exceptionally(ex -> {
                log.error("Error when handling bind request:{}, real exception is:", bindData, ex);
                bindFuture.completeExceptionally(ex);
                return null;
            });

        return bindFuture;
    }

    @Override
    public CompletableFuture<Void> unbind(String tenant, String namespace, String exchangeName, String queueName,
                                          String bindingKey, Map<String, Object> arguments) {
        final BindData bindData = new BindData(exchangeName, tenant, namespace, queueName,
                EXCHANGE_TYPE, bindingKey, arguments, bindingKey);
        CompletableFuture<Void> bindFuture = new CompletableFuture<>();
        exchangeService.getExchangeAsync(exchangeName, tenant, namespace)
            .thenAcceptBoth(queueService.getQueue(queueName, tenant, namespace), (exchangeOps, queueOps) -> {
                // smart as me ^_^
                if (exchangeOps.isEmpty()) {
                    log.error("Error when handling bind request:{}, exchange not found", bindData);
                    bindFuture.completeExceptionally(new NotFoundException.ExchangeNotFoundException());
                    return;
                } else if (queueOps.isEmpty()) {
                    log.error("Error when handling bind request:{}, queue not found", bindData);
                    bindFuture.completeExceptionally(new NotFoundException.QueueNotFoundException());
                    return;
                }
                Exchange exchange = exchangeOps.get();
                Queue queue = queueOps.get();
                unbindFromExchange(exchange, bindData).thenAcceptBoth(unbindFromQueue(queue, bindData), (e, q) -> {
                    bindFuture.complete(null);
                }).exceptionally(ex -> {
                    Throwable realEx = FutureExceptionUtils.decodeFuture(ex);
                    if (ex instanceof NotFoundException.BindNotFoundException) {
                        log.warn("Bind data:{} not found", bindData);
                        bindFuture.complete(null);
                    } else {
                        log.error("Exception when updating metadata,", realEx);
                        bindFuture.completeExceptionally(realEx);
                    }
                    return null;
                });
            });
        return bindFuture;
    }

    @Override
    public CompletableFuture<Void> unbindAllFromExchange(Set<BindData> bindDataSet) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        List<CompletableFuture<Optional<Exchange>>> futures = new ArrayList<>();
        bindDataSet.forEach(bindData -> {
            futures.add(exchangeService.getExchangeAsync(bindData.getSource(), bindData.getTenant(), bindData.getVhost())
                .whenComplete((exchangeOpt, ex) -> {
                    if (ex != null) {
                        log.error("Failed to remove bind :{} from exchange metadata, cause queue delete", bindData, ex);
                        return;
                    }
                    if (exchangeOpt.isPresent()) {
                        log.info("Remove bind :{} from exchange metadata, cause queue delete", bindData);
                        unbindFromExchange(exchangeOpt.get(), bindData);
                    }
                }));
        });
        FutureUtil.waitForAll(futures).whenComplete((__, ex) -> {
            if (ex != null) {
                log.error("Exception when deleting queue and unbind exchanges which has its bind-relationship:", ex);
                future.completeExceptionally(ex.getCause());
                return;
            }
            future.complete(null);
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> unbindAllFromQueue(Set<BindData> bindDataSet) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        List<CompletableFuture<Optional<Queue>>> futures = new ArrayList<>();
        bindDataSet.forEach(bindData -> {
            futures.add(queueService.getQueue(bindData.getDestination(), bindData.getTenant(), bindData.getVhost())
                .whenComplete((queueOpt, ex) -> {
                    if (ex != null) {
                        log.error("Failed to remove bind :{} from queue metadata, cause exchange delete", bindData, ex);
                        return;
                    }
                    if (queueOpt.isPresent()) {
                        log.info("Remove bind :{} from queue metadata, cause exchange delete", bindData);
                        unbindFromQueue(queueOpt.get(), bindData);
                    }
                }));
        });
        FutureUtil.waitForAll(futures).whenComplete((__, ex) -> {
            if (ex != null) {
                log.error("Exception when deleting exchange and unbind queues which has its bind-relationship:", ex);
                future.completeExceptionally(ex.getCause());
                return;
            }
            future.complete(null);
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> bindToExchange(Exchange exchange, BindData bindData) {
        String path = ExchangeServiceImpl.generateExchangePath(exchange.getTenant(), exchange.getVhost(), exchange.getName());
        ResourceLockServiceImpl.acquireResourceLock(path);
        return exchange.addBindData(bindData).thenCompose(__ -> {
            return metadataService.modifyUpdateMetadata(ExchangeData.class, path, data -> {
                return exchange.getExchangeData();
            }).whenComplete((ignore, ex) -> {
                ResourceLockServiceImpl.releaseResourceLock(path);
                if (ex != null) {
                    exchange.removeBindData(bindData);
                }
            });
        });
    }

    @Override
    public CompletableFuture<Void> unbindFromExchange(Exchange exchange, BindData bindData) {
        String path = ExchangeServiceImpl.generateExchangePath(exchange.getTenant(), exchange.getVhost(), exchange.getName());
        ResourceLockServiceImpl.acquireResourceLock(path);
        return exchange.removeBindData(bindData).thenCompose(__ -> {
            return metadataService.modifyUpdateMetadata(ExchangeData.class, path, data -> {
                return exchange.getExchangeData();
            }).whenComplete((ignore, ex) -> {
                ResourceLockServiceImpl.releaseResourceLock(path);
                if (ex != null) {
                    exchange.addBindData(bindData);
                    return;
                }
                if (exchange.isAutoDelete() && exchange.getBindData().size() == 0) {
                    exchangeService.removeExchangeAsync(exchange.getName(), exchange.getTenant(), exchange.getVhost());
                }
            });
        });
    }

    @Override
    public CompletableFuture<Void> bindToQueue(Queue queue, BindData newData) {
        String path = QueueServiceImpl.generateQueuePath(queue.getTenant(), queue.getVhost(), queue.getName());
        ResourceLockServiceImpl.acquireResourceLock(path);
        return queue.addBindData(newData).thenCompose(__ -> {
            return metadataService.modifyUpdateMetadata(QueueData.class, path, data -> {
                return queue.getQueueData();
            }).whenComplete((ignore, ex) -> {
                ResourceLockServiceImpl.releaseResourceLock(path);
                if (ex != null) {
                    queue.removeBindData(newData);
                }
            });
        });
    }

    @Override
    public CompletableFuture<Void> unbindFromQueue(Queue queue, BindData newData) {
        String path = QueueServiceImpl.generateQueuePath(queue.getTenant(), queue.getVhost(), queue.getName());
        ResourceLockServiceImpl.acquireResourceLock(path);
        return queue.removeBindData(newData).thenCompose(__ -> {
            return metadataService.modifyUpdateMetadata(QueueData.class, path, data -> {
                return queue.getQueueData();
            }).whenComplete((ignore, ex) -> {
                ResourceLockServiceImpl.releaseResourceLock(path);
                if (ex != null) {
                    queue.addBindData(newData);
                }
            });
        });
    }

    @Override
    public CompletableFuture<Integer> checkExchangeBound(String tenant, String namespace,
                                                         String exchangeName, String queueName, String routingKey) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        exchangeService.getExchangeAsync(exchangeName, tenant, namespace)
            .thenAcceptBoth(queueService.getQueue(queueName, tenant, namespace), (exchangeOpt, queueOpt) -> {
                if (exchangeOpt.isEmpty()) {
                    future.complete(ExchangeBoundOkBody.EXCHANGE_NOT_FOUND);
                    return;
                }
                if (queueOpt.isEmpty()) {
                    future.complete(ExchangeBoundOkBody.QUEUE_NOT_FOUND);
                    return;
                }
                if (queueOpt.get().getBindData().size() == 0) {
                    future.complete(ExchangeBoundOkBody.QUEUE_NOT_BOUND);
                }
                int code = ExchangeBoundOkBody.NO_QUEUE_BOUND_WITH_RK;
                Set<BindData> bindDataSet = new HashSet<>(exchangeOpt.get().getBindData());
                for (BindData bindData : bindDataSet) {
                    if (bindData.getDestination().equals(queueName)) {
                        code = ExchangeBoundOkBody.SPECIFIC_QUEUE_NOT_BOUND_WITH_RK;
                        if (bindData.getRoutingKey().equals(routingKey)) {
                            code = ExchangeBoundOkBody.OK;
                            break;
                        }
                    }
                }
                future.complete(code);
            }).exceptionally(ex -> {
                log.error("Exception when check bound of exchange:{} with queue:{}", exchangeName, queueName);
                future.completeExceptionally(ex.getCause());
                return null;
            });
        return future;
    }
//
//    // process delete exchange -> unbind from queue/ delete queue -> unbind from exchange.
//    private void handleNotification(Notification notification) {
//        if (notification.getType() != NotificationType.Deleted) {
//            return;
//        }
//        String path = notification.getPath();
//        // Delete namespace forcefully, cache misses everytime, so checking zk everytime, thus it is slow.
//        BiConsumer<NamespaceName, BiConsumer<Notification, NamespaceName>> consumer = (namespaceName, notice) -> {
//            brokerService.getPulsarService().getPulsarResources().getNamespaceResources()
//                .getPoliciesAsync(namespaceName)
//                .thenAccept(opt -> {
//                    if (opt.isEmpty()) {
//                        return;
//                    }
//                    notice.accept(notification, namespaceName);
//                });
//        };
//        if (path.startsWith(EXCHANGE_PREFIX) && path.split("/").length == 6) {
//            consumer.accept(NamespaceName.get(path.split("/")[3], path.split("/")[4]),
//                    this::handleExchangeDeleteUnbind);
//            return;
//        }
//        if (path.startsWith(MANAGED_LEDGER)) {
//            consumer.accept(NamespaceName.get(path.split("/")[2], path.split("/")[3]),
//                this::handleQueueDeleteUnbind);
//        }
//    }
//
//    private void handleExchangeDeleteUnbind(Notification notification, NamespaceName namespaceName) {
//        String exchangeName =
//    }
//
//    private void handleQueueDeleteUnbind(Notification notification, NamespaceName namespaceName) {
//
//    }
}
