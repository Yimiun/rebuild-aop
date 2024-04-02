package io.yuan.pulsar.handlers.amqp.metadata;

import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.metadata.api.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

@Slf4j
public class MetadataServiceImpl implements MetadataService{

    private final MetadataStore metadataStore;

    private final CopyOnWriteArrayList<Consumer<Notification>> listeners = new CopyOnWriteArrayList<>();

    protected final ScheduledExecutorService executor;

    private final Map<Class, MetadataCache> classMetadataCache = new ConcurrentHashMap<>();

    public MetadataServiceImpl(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
        this.executor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("amqp-metadata"));
        metadataStore.registerListener(notification -> {
            executor.submit(() -> {
                try {
                    listeners.forEach(listener -> listener.accept(notification));
                } catch (Exception e) {
                    log.error("Error in call back metadata listeners", e);
                }
            });
        });
    }

    @Override
    public <T> CompletableFuture<Void> updateTopicMetadata(Class<T> clazz, T metadata, String path) {
        return createOrGetMetadataCache(clazz).create(path, metadata);
    }

    @Override
    public void registerListener(Consumer<Notification> listener) {
        listeners.add(listener);
    }

    @Override
    public <T> CompletableFuture<Optional<T>> getTopicMetadata(Class<T> clazz, String path) {
        return createOrGetMetadataCache(clazz).get(path);
    }

    @Override
    public <T> MetadataCache<T> createOrGetMetadataCache(Class<T> clazz) {
        return classMetadataCache.computeIfAbsent(clazz, key -> metadataStore.getMetadataCache(clazz));
    }
}
