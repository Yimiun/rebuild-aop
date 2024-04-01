package io.yuan.pulsar.handlers.amqp.metadata;

import org.apache.pulsar.metadata.api.MetadataSerde;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.Stat;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public interface MetadataService {

    <T> CompletableFuture<Stat> updateTopicMetadata(MetadataSerde<T> serde, T metadata, String path);

    void registerListener(Consumer<Notification> listener) throws IOException;

    <T> CompletableFuture<T> getTopicMetadata(MetadataSerde<T> serde, String path);
}
