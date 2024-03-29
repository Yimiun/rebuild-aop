package io.yuan.pulsar.handlers.amqp.amqp.component;

import java.util.Map;

public abstract class AbstractExchange implements Exchange{

    protected final String exchangeName;
    protected final Exchange.Type exchangeType;
    protected final boolean durable;
    protected final boolean autoDelete;
    protected final boolean internal;
    protected Map<String, Object> arguments;

    AbstractExchange(String exchangeName, Type type, boolean durable,
                       boolean autoDelete, boolean internal, Map<String, Object> arguments) {
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
}
