package io.yuan.pulsar.handlers.amqp.metadata;

import java.util.concurrent.CompletableFuture;

public interface Listener {

    CompletableFuture<Void> onCreate();

    CompletableFuture<Void> onChange();

    CompletableFuture<Void> onDelete();
}
