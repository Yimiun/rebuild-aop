package io.yuan.pulsar.handlers.amqp.amqp.component.message;

public interface AmqpMessage {

    int getDeliveryCount();

    void incrementDeliveryCount();

    void decrementDeliveryCount();

    void setRedelivered();

    boolean isRedelivered();

    boolean isExpired();

    boolean isDurable();

    long getExpiredTime();

    byte getPriority();

}
