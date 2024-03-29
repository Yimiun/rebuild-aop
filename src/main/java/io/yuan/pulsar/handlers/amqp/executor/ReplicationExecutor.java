package io.yuan.pulsar.handlers.amqp.executor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ReplicationExecutor {

    private volatile static ReplicationExecutor instance;

    private final ExecutorService executorService;

    private ReplicationExecutor() {
        int numberOfProcessors = Runtime.getRuntime().availableProcessors();
        executorService = Executors.newFixedThreadPool(numberOfProcessors);
    }

    public static ReplicationExecutor getInstance() {
        if (instance == null) {
            synchronized (ReplicationExecutor.class) {
                if (instance == null) {
                    instance = new ReplicationExecutor();
                }
            }
        }
        return instance;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }
}
