package io.yuan.pulsar.handlers.amqp.amqp.factory;

import io.yuan.pulsar.handlers.amqp.amqp.component.AbstractExchange;
import io.yuan.pulsar.handlers.amqp.amqp.service.TopicService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ExchangeFactoryImpl implements ExchangeFactory{

    private final TopicService amqpBrokerService;

    private final Map<String, AbstractExchange> map = new ConcurrentHashMap<>();

    ExchangeFactoryImpl(TopicService exchangeService) {
        this.amqpBrokerService = exchangeService;
    }
}
