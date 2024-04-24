package io.yuan.pulsar.handlers.amqp.amqp.component.exchange;

import io.yuan.pulsar.handlers.amqp.amqp.pojo.BindData;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.ExchangeData;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 所有方法前都需要加入CAS状态判断
 * */
public class PersistentExchange extends AbstractExchange {

    public PersistentExchange(ExchangeData exchangeData) {
        super(exchangeData);

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
