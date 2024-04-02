package io.yuan.pulsar.handlers.amqp.proxy.protocol;

public class ByteCaster {

    public static byte[] fromIntToBytes(int number) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) ((number >> 24) & 0xFF);
        bytes[1] = (byte) ((number >> 16) & 0xFF);
        bytes[2] = (byte) ((number >> 8) & 0xFF);
        bytes[3] = (byte) ((number) & 0xFF);
        return bytes;
    }

    public static byte[] fromLongToBytes(long number) {
        byte[] bytes = new byte[8];
        bytes[0] = (byte) ((number >> 56) & 0xFF);
        bytes[1] = (byte) ((number >> 48) & 0xFF);
        bytes[2] = (byte) ((number >> 40) & 0xFF);
        bytes[3] = (byte) ((number >> 32) & 0xFF);
        bytes[4] = (byte) ((number >> 24) & 0xFF);
        bytes[5] = (byte) ((number >> 16) & 0xFF);
        bytes[6] = (byte) ((number >> 8) & 0xFF);
        bytes[7] = (byte) ((number) & 0xFF);
        return bytes;
    }

    public static int fromBytesToInt(byte[] bytes) {
        return ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16) | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
    }

    public static long fromBytesToLong(byte[] bytes) {
        return  ((long) (bytes[0] & 0xFF) << 56) |
            ((long) (bytes[1] & 0xFF) << 48) |
            ((long) (bytes[2] & 0xFF) << 40) |
            ((long) (bytes[3] & 0xFF) << 32) |
            ((long) (bytes[4] & 0xFF) << 24) |
            ((long) (bytes[5] & 0xFF) << 16) |
            ((long) (bytes[6] & 0xFF) << 8 ) |
            ((long) (bytes[7] & 0xFF) );
    }

}
