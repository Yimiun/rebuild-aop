package io.yuan.pulsar.handlers.amqp.cache;

import io.netty.buffer.ByteBuf;
import io.yuan.pulsar.handlers.amqp.amqp.component.AbstractExchange;
import io.yuan.pulsar.handlers.amqp.amqp.component.Exchange;
import io.yuan.pulsar.handlers.amqp.configuration.AmqpServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;

import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 路由队列，保存顺序发送的消息，非持久化。仅仅用于队列
 * 交换->复用channel(不走5672，再启动一个轻量级的端口，5672的收发消息太慢了，直接传送bytebuffer，自定义协议)->
 *
 * 该类有两个队列：主队列与备用队列
 *
 * 每个队列设置最大缓存大小，缓存，消费先从该队列消费，由策略，根据成员变量消费位点写入Topic，并从队列里移除已落盘消息
 * (大小由bytebuf的大小填充对象头大小近似代替，在大流量情况下差不了多少)
 * 若在写入盘过程中 来了大量新的写入请求：
 *
 * (1)根据Stat 判断出正在写入状态，无法存储到当前队列，存储到内部的另一个备用队列中。这个队列会在
 * 原队列清空后立刻交换指针，并且同样落盘。
 *
 * 副队列落盘清空后继续恢复Caching状态，但是恢复前需要检查是否执行(1)逻辑，即是否又触发了阈值
 *
 * CacheSize应当根据现在的所有queue流量情况动态更改，对于大流量queue, 提升thresholdSize大小
 *
 * 若出现主队列在存盘时，备用队列被写满（写不满，只能内存溢出，这里指额定上限*1.2 (?存疑)），则放弃所有策略，全部采用立即落盘。
 *
 * 只有目前有消费者
 * */
public class RoutingQueue {

    private State state;

    private final Queue<ByteBuf> workQueue;

    private final Queue<ByteBuf> flipQueue;

    private double threshold = 0.6;

    private double cacheSize = 2 * 1024 * 1024;

    private double thresholdSize = threshold * cacheSize;

    private enum State {
        // 主队列未达到阈值，副队列为空
        CACHING,
        // 主队列正在落盘或副队列正在落盘
        FENCED,
        // 主队列存盘，备用队列满了，
        BROKEN,
        // exchange删了/出问题了
        CLOSE;
    }

    public RoutingQueue(AbstractExchange exchange, ExecutorService executorService, double thresholdRate,
                        double cacheSize, PersistentTopic queueTopic) {
        workQueue = new LinkedBlockingQueue<>();
        flipQueue = new LinkedBlockingQueue<>();
    }
}
