package io.yuan.pulsar.handlers.amqp.proxy.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
//import org.apache.qpid.server.bytebuffer.SingleQpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.IncomingMessage;

public class ProxyToBrokerProtocolGenerator {

/**
 *         0            4         5~259
 *         +------------+----------+---------------+--------------+-----------+---------+
 *         | headLength | fileName |  AlreadyBytes | CurrentBytes | TotalBytes| payload |
 *         +------------+----------+---------------+--------------+-----------+---------+
 *            4 Bytes   0-255 Bytes    8 Bytes        4 Bytes       8 Bytes   0-99 * 1024 * (1024 - headLength)
 *            */


    public static ByteBuf generateProtocol(IncomingMessage income) {
//        if (income.getBodyCount() > 0) {
//            int headerLength = income.getBodyCount();
//            int content = income
//            ByteBuf byteBuf = Unpooled.buffer();
//            byteBuf.writeBytes(ByteCaster.fromIntToBytes(headerLength));
//            for (int i = 0; i < income.getBodyCount(); i++) {
//                byteBuf.writeBytes(income.getContentChunk(i).getPayload().array());
//            }
//            byte[] bytes = new byte[byteBuf.writerIndex()];
//            byteBuf.readBytes(bytes, 0, byteBuf.writerIndex());
//        }
        return null;
    }
}
