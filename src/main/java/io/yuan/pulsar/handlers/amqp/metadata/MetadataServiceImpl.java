package io.yuan.pulsar.handlers.amqp.metadata;

import io.netty.util.concurrent.DefaultThreadFactory;
import io.vertx.core.impl.ConcurrentHashSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.metadata.api.MetadataSerde;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.Stat;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

@Slf4j
public class MetadataServiceImpl implements MetadataService{

    private final MetadataStore metadataStore;

    private final CopyOnWriteArrayList<Consumer<Notification>> listeners = new CopyOnWriteArrayList<>();

    protected final ScheduledExecutorService executor;

    MetadataServiceImpl(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
        this.executor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("amqp-metadata"));
        metadataStore.registerListener(notification -> {
            executor.submit(() -> {
                listeners.forEach(listener -> listener.accept(notification));
            });
        });
    }

    @Override
    public <T> CompletableFuture<Stat> updateTopicMetadata(MetadataSerde<T> serde, T metadata, String name) {
        try {
            byte[] ser = serde.serialize(name, metadata);
            return metadataStore.put(name, ser, Optional.of(-1L));
        } catch (IOException e) {
            log.error("serde failed", e);

        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void registerListener(Consumer<Notification> listener) {
        listeners.add(listener);
    }

    @Override
    public <T> CompletableFuture<T> getTopicMetadata(MetadataSerde<T> serde, String path) {
        return metadataStore.get(path).thenCompose((opRes) -> {
                if (!opRes.isPresent()) {
                    return CompletableFuture.completedFuture(null);
                }
                try {
                    T t = serde.deserialize(path, opRes.get().getValue(), null);
                    return CompletableFuture.completedFuture(t);
                } catch (IOException e) {
                    log.error("serde failed", e);
                }
                return CompletableFuture.completedFuture(null);
            });
    }
}
