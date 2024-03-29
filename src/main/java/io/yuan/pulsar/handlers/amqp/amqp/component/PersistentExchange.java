package io.yuan.pulsar.handlers.amqp.amqp.component;

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class PersistentExchange extends AbstractExchange {

    private final PersistentTopic topic;

    PersistentExchange(String exchangeName, Type type, PersistentTopic persistentTopic, boolean durable,
                       boolean autoDelete, boolean internal, Map<String, Object> arguments) {
        super(exchangeName, type, durable, autoDelete, internal, arguments);
        this.topic = persistentTopic;
    }

    @Override
    public Topic getTopic() {
        return null;
    }

    @Override
    public CompletableFuture<Position> writeToTopic(ByteBuf buf) {
        topic.getManagedLedger().getCursors().forEach(managedCursor -> {
//            managedCursor.getReadPosition();
        });
        return null;
    }

    @Override
    public CompletableFuture<Boolean> close() {
        return null;
    }
}
