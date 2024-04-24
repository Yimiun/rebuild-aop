package io.yuan.pulsar.handlers.amqp.utils;

import java.util.concurrent.CompletionException;

public class FutureExceptionUtils {

    public static Throwable decodeFuture(Throwable e) {
        if (e instanceof CompletionException) {
            return decodeFuture(e.getCause());
        }
        return e;
    }
}
