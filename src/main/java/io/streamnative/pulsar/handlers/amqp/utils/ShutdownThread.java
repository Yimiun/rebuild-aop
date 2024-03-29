package io.streamnative.pulsar.handlers.amqp.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class ShutdownThread extends Thread{

    private final boolean isInterrupted;
    private final String logIdent;
    private final CountDownLatch shutdownInitiated = new CountDownLatch(1);
    private final CountDownLatch shutdownComplete = new CountDownLatch(1);

    public ShutdownThread(String name) {
        this(name, true);
    }

    public ShutdownThread(String name,
                              boolean isInterrupted) {
        super(name);
        this.isInterrupted = isInterrupted;
        this.setDaemon(false);
        this.logIdent = "[" + name + "]";
    }

    public boolean isRunning() {
        return shutdownInitiated.getCount() != 0;
    }

    public void shutdown() throws InterruptedException {
        initiateShutdown();
        awaitShutdown();
    }

    public boolean isShutdownComplete() {
        return shutdownComplete.getCount() == 0;
    }

    public synchronized boolean initiateShutdown() {
        if (isRunning() && log.isDebugEnabled()) {
            log.debug("{} Shutting down", logIdent);
        }
        shutdownInitiated.countDown();
        if (isInterrupted) {
            interrupt();
            return true;
        } else {
            return false;
        }
    }

    /**
     * After calling initiateShutdown(), use this API to wait until the shutdown is complete.
     */
    public void awaitShutdown() throws InterruptedException {
        shutdownComplete.await();
        if (log.isDebugEnabled()) {
            log.debug("{} Shutdown completed", logIdent);
        }
    }

    /**
     *  Causes the current thread to wait until the shutdown is initiated,
     *  or the specified waiting time elapses.
     *
     * @param timeout
     * @param unit
     */
    public void pause(long timeout, TimeUnit unit) throws InterruptedException {
        if (shutdownInitiated.await(timeout, unit)) {
            if (log.isTraceEnabled()) {
                log.trace("{} shutdownInitiated latch count reached zero. Shutdown called.", logIdent);
            }
        }
    }

    /**
     * This method is repeatedly invoked until the thread shuts down or this method throws an exception.
     */
    protected abstract void doWork();

    @Override
    public void run() {
        if (log.isDebugEnabled()) {
            log.debug("{} Starting", logIdent);
        }
        try {
            while (isRunning()) {
                doWork();
            }
        } catch (Exception e) {
            shutdownInitiated.countDown();
            shutdownComplete.countDown();
            if (log.isDebugEnabled()) {
                log.debug("{} Stopped", logIdent);
            }
//            Exit.exit(e.statusCode());
        } catch (Throwable cause) {
            if (isRunning()) {
                log.error("{} Error due to", logIdent, cause);
            }
        } finally {
            shutdownComplete.countDown();
        }
    }

}
