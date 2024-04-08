package io.yuan.pulsar.handlers.amqp.admin;

import io.yuan.pulsar.handlers.amqp.AmqpProtocolHandler;
import io.yuan.pulsar.handlers.amqp.amqp.service.TopicService;
import io.yuan.pulsar.handlers.amqp.proxy.lookup.AmqpLookupHandler;
import io.yuan.pulsar.handlers.amqp.proxy.lookup.AmqpLookupHandlerImpl;

import javax.servlet.ServletContext;
import javax.ws.rs.core.Context;

public class BaseResource {

    @Context
    protected ServletContext servletContext;

    private TopicService topicService;

    private AmqpProtocolHandler amqpProtocolHandler;

    private AmqpLookupHandler amqpLookupHandler;

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

    protected AmqpLookupHandler getAmqpLookupHandler() {
        if (amqpLookupHandler == null) {
            amqpLookupHandler = aop().getAmqpBrokerService().getAmqpLookupHandler();
        }
        return amqpLookupHandler;
    }
}
