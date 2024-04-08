package io.yuan.pulsar.handlers.amqp.broker;

import io.yuan.pulsar.handlers.amqp.amqp.service.ExchangeService;
import io.yuan.pulsar.handlers.amqp.amqp.service.ExchangeServiceImpl;
import io.yuan.pulsar.handlers.amqp.amqp.service.TopicService;
import io.yuan.pulsar.handlers.amqp.configuration.AmqpServiceConfiguration;
import io.yuan.pulsar.handlers.amqp.metadata.MetadataService;
import io.yuan.pulsar.handlers.amqp.metadata.MetadataServiceImpl;
import io.yuan.pulsar.handlers.amqp.proxy.BundleListener;
import io.yuan.pulsar.handlers.amqp.proxy.TopicOwnershipListener;
//import io.yuan.pulsar.handlers.amqp.proxy.lookup.AmqpLookupHandler;
//import io.yuan.pulsar.handlers.amqp.proxy.lookup.AmqpLookupHandlerImpl;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.naming.TopicName;

public class AmqpBrokerService {

    @Getter
    public PulsarService pulsarService;
    @Getter
    AmqpServiceConfiguration amqpServiceConfiguration;
    @Getter
    TopicService topicService;

    ExchangeService exchangeService;

    MetadataService metadataService;

    BundleListener bundleListener;

//    @Getter
//    private AmqpLookupHandler amqpLookupHandler;

    public AmqpBrokerService(PulsarService pulsar, AmqpServiceConfiguration amqpConfig) {
        this.pulsarService = pulsar;
        this.amqpServiceConfiguration = amqpConfig;
        this.metadataService = new MetadataServiceImpl(pulsarService.getLocalMetadataStore());
        this.topicService = new TopicService(metadataService, pulsarService);
        this.exchangeService = new ExchangeServiceImpl(metadataService, amqpServiceConfiguration);
        this.bundleListener = new BundleListener(pulsarService.getNamespaceService());
//        this.amqpLookupHandler = new AmqpLookupHandlerImpl(pulsarService, amqpConfig, metadataService, topicService);
        bundleListener.addTopicOwnershipListener(new TopicOwnershipListener() {
            @Override
            public void whenLoad(TopicName topicName) {
                if (!topicName.isPersistent()) {
                    // 改成queueService
//                    ((ExchangeServiceImpl)exchangeService).addNonPersistentExchange(topicName.getLocalName());
                } else {

                }
                topicService.addTopicsCache(topicName);
            }

            @Override
            public void whenUnload(TopicName topicName) {
                if (!topicName.isPersistent()) {
                    // 改成queueService
//                    ((ExchangeServiceImpl)exchangeService).removeNonPersistentExchange(topicName.getLocalName());
                } else {

                }
                topicService.removeTopicsCache(topicName);
            }

            @Override
            public String name() {
                return "handle non-persistent query";
            }
        });
    }
}
