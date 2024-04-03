package io.yuan.pulsar.handlers.amqp.broker;

import io.yuan.pulsar.handlers.amqp.amqp.service.ExchangeService;
import io.yuan.pulsar.handlers.amqp.amqp.service.ExchangeServiceImpl;
import io.yuan.pulsar.handlers.amqp.amqp.service.TopicService;
import io.yuan.pulsar.handlers.amqp.configuration.AmqpServiceConfiguration;
import io.yuan.pulsar.handlers.amqp.metadata.MetadataService;
import io.yuan.pulsar.handlers.amqp.metadata.MetadataServiceImpl;
import io.yuan.pulsar.handlers.amqp.proxy.BundleListener;
import io.yuan.pulsar.handlers.amqp.proxy.TopicOwnershipListener;
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

    public AmqpBrokerService(PulsarService pulsar, AmqpServiceConfiguration amqpConfig) {
        this.pulsarService = pulsar;
        this.amqpServiceConfiguration = amqpConfig;
        this.metadataService = new MetadataServiceImpl(pulsarService.getLocalMetadataStore());
        this.topicService = new TopicService(metadataService, pulsarService);
        this.exchangeService = new ExchangeServiceImpl(topicService, metadataService, amqpServiceConfiguration);
        this.bundleListener = new BundleListener(pulsarService.getNamespaceService());
        bundleListener.addTopicOwnershipListener(new TopicOwnershipListener() {
            @Override
            public void whenLoad(TopicName topicName) {
                if (!topicName.isPersistent()) {
                    // 改成queueService
//                    ((ExchangeServiceImpl)exchangeService).addNonPersistentExchange(topicName.getLocalName());
                }
            }

            @Override
            public void whenUnload(TopicName topicName) {
                if (!topicName.isPersistent()) {
                    // 改成queueService
//                    ((ExchangeServiceImpl)exchangeService).removeNonPersistentExchange(topicName.getLocalName());
                }
            }

            @Override
            public String name() {
                return "handle non-persistent query";
            }
        });
    }
}
