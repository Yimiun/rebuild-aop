package io.yuan.pulsar.handlers.amqp.metadata;

import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;


/**
 * use store/cache
 * There's cache map in ExchangeServiceImpl, so the function now (without hitting the cache multiple times)
 * is to use the cache as an Advanced-MetaDataStore which providing an auto-serde method
 *
 * In the future, the cache will be hit multiple times in the AMQP-ADMIN query and other functions,
 * so currently, I don't think using cache is "too forceful".
 *
 * But if necessary, it would be replaced with MetaDataStore in future
 * */

@Slf4j
public class MetadataServiceImpl implements MetadataService {

    private final MetadataStore metadataStore;

    private final CopyOnWriteArrayList<Consumer<Notification>> listeners = new CopyOnWriteArrayList<>();

    private final ScheduledExecutorService executor;

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
    public <T> CompletableFuture<Void> updateMetadata(Class<T> clazz, T metadata, String path, boolean refresh) {
        MetadataCache<T> metadataCache = createOrGetMetadataCache(clazz);
        CompletableFuture<Void> completableFuture = metadataCache.create(path, metadata);
        if (refresh) {
            metadataCache.refresh(path);
            return completableFuture.thenApplyAsync(__ -> __, executor);
        }
        return completableFuture;
    }

    @Override
    public <T> CompletableFuture<Optional<T>> getMetadata(Class<T> clazz, String path, boolean refresh) {
        MetadataCache<T> metadataCache = createOrGetMetadataCache(clazz);
        if (refresh) {
            metadataCache.refresh(path);
            return metadataCache.get(path).thenApplyAsync(__ -> __, executor);
        }
        return metadataCache.get(path);
    }

    @Override
    public <T> MetadataCache<T> createOrGetMetadataCache(Class<T> clazz) {
        return classMetadataCache.computeIfAbsent(clazz, key -> metadataStore.getMetadataCache(clazz));
    }

    @Override
    public CompletableFuture<Void> deleteMetadata(String path) {
        return metadataStore.delete(path, Optional.of(-1L));
    }

    @Override
    public <T> void invalidPath(Class<T> clazz, String path) {
        createOrGetMetadataCache(clazz).invalidate(path);
    }

    @Override
    public CompletableFuture<List<String>> getChildrenList(String parentPath) {
        return metadataStore.getChildren(parentPath);
//        CompletableFuture<List<String>> res = new CompletableFuture<>();
//        metadataStore.getChildren(parentPath).thenAccept(list -> {
//            executor.submit(()->{
//               res.complete(list);
//            });
//        });
//        return res;
    }

    @Override
    public <T> CompletableFuture<List<T>> getAllChildrenData(String parentPath, Class<T> childClass) {
        return getChildrenList(parentPath)
            .thenComposeAsync(nodeList -> {
                List<CompletableFuture<Optional<T>>> res = nodeList.stream()
                    .map(nodePath -> String.format("%s%s", parentPath, nodePath))
                    .map(path -> getMetadata(childClass, path, true))
                    .collect(Collectors.toUnmodifiableList());
                return FutureUtil.waitForAll(res)
                    .thenApply(__ -> {
                       return res.stream()
                           .map(CompletableFuture::join)
                           .filter(Optional::isPresent)
                           .map(Optional::get)
                           .collect(Collectors.toUnmodifiableList());
                    });
            }, executor);
    }

    @Override
    public CompletableFuture<Void> deleteMetadataRecursive(String path) {
        return metadataStore.deleteRecursive(path);
    }

    @Override
    public void registerListener(Consumer<Notification> listener) {
        listeners.add(listener);
    }
}
