package io.yuan.pulsar.handlers.amqp.metadata;

import org.apache.pulsar.metadata.api.*;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public interface MetadataService {

    // create or update
    <T> CompletableFuture<Void> updateMetadata(Class<T> clazz, T metadata, String path, boolean refresh);

    // register listeners
    void registerListener(Consumer<Notification> listener);

    // get
    <T> CompletableFuture<Optional<T>> getMetadata(Class<T> clazz, String path, boolean refresh);

    // create or get Metadata reference
    <T> MetadataCache<T> createOrGetMetadataCache(Class<T> clazz);

    // delete
    CompletableFuture<Void> deleteMetadata(String path);

    <T> void invalidPath(Class<T> clazz, String path);

    CompletableFuture<List<String>> getChildrenList(String parentPath);

    <T> CompletableFuture<List<T>> getAllChildrenData(String parentPath, Class<T> childClass);

    CompletableFuture<Void> deleteMetadataRecursive(String path);
}
