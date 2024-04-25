package io.yuan.pulsar.handlers.amqp.amqp.component.exchange;

import io.netty.buffer.ByteBuf;
import io.yuan.pulsar.handlers.amqp.amqp.component.State;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.BindData;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.ExchangeData;
import io.yuan.pulsar.handlers.amqp.exception.AmqpExchangeException;
import io.yuan.pulsar.handlers.amqp.exception.NotFoundException;
import io.yuan.pulsar.handlers.amqp.exception.ServiceRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.TopicName;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public abstract class AbstractExchange implements Exchange {

    protected final ExchangeData exchangeData;
    protected final String tenant;
    protected final String namespace;
    protected final String exchangeName;
    protected final Exchange.Type exchangeType;
    protected final boolean durable;
    protected final boolean autoDelete;
    protected final boolean internal;
    protected Map<String, Object> arguments;
    protected volatile State exchangeState = State.Closed;
    protected final ReentrantReadWriteLock bindLock = new ReentrantReadWriteLock();
    protected final Set<BindData> bindData;
    protected final Map<String, Set<BindData>> routerMap = new ConcurrentHashMap<>();
    protected static final AtomicReferenceFieldUpdater<AbstractExchange, State> stateReference =
        AtomicReferenceFieldUpdater.newUpdater(AbstractExchange.class, State.class, "exchangeState");

    AbstractExchange(ExchangeData exchangeData) {
        this.exchangeData = exchangeData;
        this.tenant = exchangeData.getTenant();
        this.namespace = exchangeData.getVhost();
        this.exchangeName = exchangeData.getName();
        this.exchangeType = Type.value(exchangeData.getType());
        this.durable = exchangeData.isDurable();
        this.autoDelete = exchangeData.isAutoDelete();
        this.internal = exchangeData.isInternal();
        this.arguments = exchangeData.getArguments();
        this.bindData = exchangeData.getBindsData();
        start();
        generateBindData(bindData);
    }

    @Override
    public CompletableFuture<Void> route(TopicName from, ByteBuf buf, String routingKey) {
        try {
            bindLock.readLock().lock();

            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            if (exchangeState.equals(State.Closed)) {
                log.warn("Route stopped, because the exchange:{} is deleted", exchangeName);
                completableFuture.completeExceptionally(new IOException());
            }
            if (!exchangeName.equals(from.getLocalName())) {
                completableFuture.completeExceptionally(new IOException());
            }
            return completableFuture;
        } finally {
            bindLock.readLock().unlock();
        }
    }

    private void generateBindData(Set<BindData> bindData) {
        try {
            bindLock.writeLock().lock();
            bindData.forEach(bd -> {
                routerMap.computeIfAbsent(bd.getRoutingKey(), key -> new HashSet<>()).add(bd);
            });
        } finally {
            bindLock.writeLock().unlock();
        }
    }

    @Override
    public CompletableFuture<Void> addBindData(BindData bindData) {
        if (exchangeState.equals(State.Closed)) {
            return CompletableFuture.failedFuture(new AmqpExchangeException.ExchangeAlreadyClosedException("Exchange closed"));
        }
        try {
            bindLock.writeLock().lock();
            CompletableFuture<Void> future = new CompletableFuture<>();
            if (this.bindData.add(bindData) &&
                routerMap.computeIfAbsent(bindData.getRoutingKey(), key -> new HashSet<>()).add(bindData)) {

                future.complete(null);
            } else {
                log.warn("Duplicate bind data:{}", bindData);
                // same bindData exist, don't update metadata
                future.completeExceptionally(new ServiceRuntimeException.DuplicateBindException("Duplicate bind data"));
            }
            return future;
        } finally {
            bindLock.writeLock().unlock();
        }
    }

    @Override
    public CompletableFuture<Void> removeBindData(BindData bindData) {
        if (exchangeState.equals(State.Closed)) {
            return CompletableFuture.failedFuture(new AmqpExchangeException.ExchangeAlreadyClosedException("Exchange closed"));
        }
        try {
            bindLock.writeLock().lock();
            CompletableFuture<Void> future = new CompletableFuture<>();
            if (this.bindData.remove(bindData)) {
                routerMap.computeIfPresent(bindData.getRoutingKey(), (k, v) -> {
                    if (v.remove(bindData)) {
                        future.complete(null);
                    } else {
                        future.completeExceptionally(new NotFoundException.BindNotFoundException("Bind data not found"));
                    }
                    return v;
                });
            } else {
                future.completeExceptionally(new NotFoundException.BindNotFoundException("Bind data not found"));
            }
            return future;
        } finally {
            bindLock.writeLock().unlock();
        }
    }

    @Override
    public CompletableFuture<Void> close() {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        if (stateReference.compareAndSet(this, State.On, State.Closed)) {
            routerMap.clear();
            arguments.clear();
            // Reduce the reference count of active routing related Pulsar Producers
            completableFuture.complete(null);
        } else {
            completableFuture.completeExceptionally(new IOException("Already close"));
        }
        return completableFuture;
    }

    @Override
    public void start() {
        if (stateReference.compareAndSet(this, State.Closed, State.On)) {
            if (log.isDebugEnabled()) {
                log.debug("Exchange:{} has been initialized", this.getName());
            }
        }
    }

    @Override
    public String getTenant() {
        return this.tenant;
    }

    @Override
    public String getVhost() {
        return this.namespace;
    }

    @Override
    public String getName() {
        return this.exchangeName;
    }

    @Override
    public boolean getDurable() {
        return this.durable;
    }

    @Override
    public boolean getAutoDelete() {
        return this.autoDelete;
    }

    @Override
    public boolean getInternal() {
        return this.internal;
    }

    @Override
    public Set<BindData> getBindData() {
        return this.bindData;
    }

    @Override
    public Map<String, Object> getArguments() {
        return this.arguments;
    }

    @Override
    public Type getType() {
        return this.exchangeType;
    }

    @Override
    public ExchangeData getExchangeData() {
        return this.exchangeData;
    }

    @Override
    public boolean isClosed() {
        return exchangeState.equals(State.Closed);
    }

    @Override
    public boolean isStart() {
        return exchangeState.equals(State.On);
    }
}
