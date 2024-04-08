package io.yuan.pulsar.handlers.amqp.amqp.component;

import io.yuan.pulsar.handlers.amqp.amqp.binding.BindData;

import java.util.List;
import java.util.Map;

/**
 * 所有方法前都需要加入CAS状态判断
 * */
public class PersistentExchange extends AbstractExchange {

    public PersistentExchange(String exchangeName, Type type, boolean durable,
                              boolean autoDelete, boolean internal, List<BindData> bindData,
                              Map<String, Object> arguments) {
        super(exchangeName, type, durable, autoDelete, internal, bindData, arguments);

    }

}
