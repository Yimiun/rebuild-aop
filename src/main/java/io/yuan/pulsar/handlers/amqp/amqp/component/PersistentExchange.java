package io.yuan.pulsar.handlers.amqp.amqp.component;

import io.netty.buffer.ByteBuf;
import io.yuan.pulsar.handlers.amqp.amqp.binding.BindData;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * 所有方法前都需要加入CAS状态判断
 * */
public class PersistentExchange extends AbstractExchange {

    public PersistentExchange(String exchangeName, Type type, boolean durable,
                              boolean autoDelete, boolean internal, BindData bindData,
                              Map<String, Object> arguments) {
        super(exchangeName, type, durable, autoDelete, internal, bindData, arguments);

    }

    @Override
    public CompletableFuture<Position> writeToTopic(ByteBuf buf) {

        return null;
    }


}
