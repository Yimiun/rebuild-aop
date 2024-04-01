package io.yuan.pulsar.handlers.amqp.listener;

import java.util.concurrent.CompletableFuture;

public interface Listener {

    CompletableFuture<Void> onCreate();

    CompletableFuture<Void> onChange();

    CompletableFuture<Void> onDelete();
}
