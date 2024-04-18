package io.yuan.pulsar.handlers.amqp.amqp.service;

import io.vertx.core.impl.ConcurrentHashSet;
import io.yuan.pulsar.handlers.amqp.metadata.MetadataService;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;

import java.util.Set;

/**
 * 提供Topic相关服务，包括元数据更新，元数据获取，根据Name返回Topic等功能
 * */
@Slf4j
public class TopicService {

    public static final String PERSISTENT_DOMAIN = TopicDomain.persistent.value() + "://";
    public static final String NON_PERSISTENT_DOMAIN = TopicDomain.non_persistent.value() + "://";

    private MetadataService metadataService;

    private final PulsarService pulsarService;

    private final Set<TopicName> ownedBundledTopics = new ConcurrentHashSet<>();

    public TopicService(MetadataService metadataService, PulsarService pulsarService) {
        this.metadataService = metadataService;
        this.pulsarService = pulsarService;
    }

    public void addTopicsCache(TopicName name) {
        ownedBundledTopics.add(name);
    }

    public void removeTopicsCache(TopicName name) {
        ownedBundledTopics.remove(name);
    }

    public void clearTopicsCache() {
        ownedBundledTopics.clear();
    }

    public boolean ownedTopic(TopicName name) {
        return ownedBundledTopics.contains(name);
    }
}
