package io.yuan.pulsar.handlers.amqp.amqp.component;

import io.netty.buffer.ByteBuf;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.BindData;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.TopicName;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

@Slf4j
public abstract class AbstractExchange implements Exchange {

    protected final String exchangeName;
    protected final Exchange.Type exchangeType;
    protected final boolean durable;
    protected final boolean autoDelete;
    protected final boolean internal;
    protected Map<String, Object> arguments;
    protected volatile State exchangeState = State.Closed;
    protected final List<BindData> bindData = new CopyOnWriteArrayList<>();
    protected final Map<String, Set<BindData>> routerMap = new ConcurrentHashMap<>();
    protected static final AtomicReferenceFieldUpdater<AbstractExchange, State> stateReference =
        AtomicReferenceFieldUpdater.newUpdater(AbstractExchange.class, State.class, "exchangeState");

    AbstractExchange(String exchangeName, Type type, boolean durable,
                     boolean autoDelete, boolean internal, List<BindData> bindData,
                     Map<String, Object> arguments) {
        this.exchangeName = exchangeName;
        this.exchangeType = type;
        this.durable = durable;
        this.autoDelete = autoDelete;
        this.internal = internal;
        this.arguments = arguments;
        this.bindData.addAll(bindData);
        start();
        generateBindData(bindData);
    }

    @Override
    public CompletableFuture<Void> route(TopicName from, ByteBuf buf, String routingKey) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        if (exchangeState.equals(State.Closed)) {
            log.warn("Route stopped, because the exchange:{} is deleted", exchangeName);
            completableFuture.completeExceptionally(new IOException());
        }
        if (!exchangeName.equals(from.getLocalName())) {
            completableFuture.completeExceptionally(new IOException());
        }
        return completableFuture;
    }

    private synchronized void generateBindData(List<BindData> bindData) {
        bindData.forEach(bd -> {
            routerMap.computeIfAbsent(bd.getRoutingKey(), key -> new HashSet<>()).add(bd);
        });
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
    public List<BindData> getBindData() {
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
    public boolean isClosed() {
        return exchangeState.equals(State.Closed);
    }

    @Override
    public boolean isStart() {
        return exchangeState.equals(State.On);
    }
}
