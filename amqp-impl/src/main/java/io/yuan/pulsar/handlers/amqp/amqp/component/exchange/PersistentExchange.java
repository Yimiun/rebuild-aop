package io.yuan.pulsar.handlers.amqp.amqp.component.exchange;

import io.yuan.pulsar.handlers.amqp.amqp.pojo.BindData;

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

    @Override
    public String toString() {
        return "PersistentExchange{" +
            "exchangeName='" + exchangeName + '\'' +
            ", exchangeType=" + exchangeType +
            ", durable=" + durable +
            ", autoDelete=" + autoDelete +
            ", internal=" + internal +
            ", arguments=" + arguments +
            ", bindData=" + bindData +
            ", routerMap=" + routerMap +
            '}';
    }
}
