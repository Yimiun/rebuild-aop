package io.yuan.pulsar.handlers.amqp.admin;

import io.yuan.pulsar.handlers.amqp.AmqpProtocolHandler;
import io.yuan.pulsar.handlers.amqp.amqp.service.ExchangeService;
import io.yuan.pulsar.handlers.amqp.amqp.service.ExchangeServiceImpl;
import io.yuan.pulsar.handlers.amqp.amqp.service.TopicService;

import javax.servlet.ServletContext;
import javax.ws.rs.core.Context;

public class BaseResource {

    @Context
    protected ServletContext servletContext;

    private TopicService topicService;

    private AmqpProtocolHandler amqpProtocolHandler;

    private ExchangeServiceImpl exchangeService;

    protected AmqpProtocolHandler aop() {
        if (amqpProtocolHandler == null) {
            amqpProtocolHandler = (AmqpProtocolHandler) servletContext.getAttribute("aop");
        }
        return amqpProtocolHandler;
    }

    protected TopicService getTopicService() {
        if (topicService == null) {
            topicService = aop().getAmqpBrokerService().getTopicService();
        }
        return topicService;
    }

    protected ExchangeServiceImpl getExchangeService() {
        if (exchangeService == null) {
            exchangeService = (ExchangeServiceImpl)aop().getAmqpBrokerService().getExchangeService();
        }
        return exchangeService;
    }

}
