package io.yuan.pulsar.handlers.amqp.amqp.service.impl;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.*;

/**
 * Design for avoiding Dirty-Reading
 * 2024/04/23 Damn it, I found a similar lock called "CallbackMutex", whatever 😑
 * */
@Slf4j
public class ResourceLockServiceImpl {

    private static final Map<String, Semaphore> threadLocks = new ConcurrentHashMap<>();

    public static void acquireResourceLock(String path) {
        try {
            threadLocks.computeIfAbsent(path, __ -> new Semaphore(1)).acquire();
        } catch (Exception e) {
            log.error("Failed to fetch resource lock for:{}, try again", path, e);
        }
    }

    public static void releaseResourceLock(String path) {
        releaseResourceLock(path, false);
    }

    public static void releaseResourceLock(String path, boolean remove) {
        threadLocks.get(path).release();
        if (remove) {
            threadLocks.remove(path);
        }
    }
}
