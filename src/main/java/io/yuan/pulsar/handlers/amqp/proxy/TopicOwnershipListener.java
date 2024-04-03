package io.yuan.pulsar.handlers.amqp.proxy;

import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;

public interface TopicOwnershipListener {

    void whenLoad(TopicName topicName);

    /**
     * It's triggered when the topic is unloaded by a broker.
     *
     * @param topicName
     */
    void whenUnload(TopicName topicName);

    /** Returns the name of the listener. */
    String name();

    default boolean test(NamespaceName namespaceName) {
        return true;
    }

}
