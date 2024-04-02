package io.yuan.pulsar.handlers.amqp.amqp.component;

import io.yuan.pulsar.handlers.amqp.amqp.binding.BindData;
import org.apache.pulsar.broker.service.Topic;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public abstract class AbstractExchange extends AbstractTopic implements Exchange {

    protected final String exchangeName;
    protected final Exchange.Type exchangeType;
    protected final boolean durable;
    protected final boolean autoDelete;
    protected final boolean internal;
    protected Map<String, Object> arguments;
    protected volatile State exchangeState = State.Closed;
    protected final Map<String, List<Topic>> routerMap = new ConcurrentHashMap<>();
    protected static final AtomicReferenceFieldUpdater<AbstractExchange, State> stateReference =
        AtomicReferenceFieldUpdater.newUpdater(AbstractExchange.class, State.class, "exchangeState");

//    protected final List<>

    AbstractExchange(String exchangeName, Type type, boolean durable,
                     boolean autoDelete, boolean internal, BindData bindData,
                     Map<String, Object> arguments, Topic topic) {
        super(topic);
        start();
        this.exchangeName = exchangeName;
        this.exchangeType = type;
        this.durable = durable;
        this.autoDelete = autoDelete;
        this.internal = internal;
        this.arguments = arguments;
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
            completableFuture.complete(null);
        } else {
            completableFuture.completeExceptionally(new IOException("Already close"));
        }
        return completableFuture;
    }

    @Override
    public CompletableFuture<Void> start() {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        if (stateReference.compareAndSet(this, State.Closed, State.On)) {
            completableFuture.complete(null);
        } else {
            completableFuture.completeExceptionally(new IOException("Already start"));
        }
        return completableFuture;
    }
}
