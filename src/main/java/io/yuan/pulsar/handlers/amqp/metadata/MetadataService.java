package io.yuan.pulsar.handlers.amqp.metadata;

import org.apache.pulsar.metadata.api.*;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public interface MetadataService {

    <T> CompletableFuture<Void> updateTopicMetadata(Class<T> clazz, T metadata, String path, boolean refresh);

    void registerListener(Consumer<Notification> listener);

    <T> CompletableFuture<Optional<T>> getTopicMetadata(Class<T> clazz, String path, boolean refresh);

    <T> MetadataCache<T> createOrGetMetadataCache(Class<T> clazz);

    <T> CompletableFuture<Void> deleteMetadata(Class<T> clazz, String path);

    <T> void invalidPath(Class<T> clazz, String path);
}
