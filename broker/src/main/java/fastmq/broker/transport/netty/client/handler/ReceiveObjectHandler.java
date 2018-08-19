package fastmq.broker.transport.netty.client.handler;

import fastmq.broker.message.KeyMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/**
 * @Description:
 * @Author: Goober Created in 2018/8/7
 */
@Sharable
public class ReceiveObjectHandler extends ByteToMessageDecoder {

    private static final int fastMqMagic = 0xfacd89;
    private static final int INT_LENGTH = 4;

    private static final ReceiveObjectHandler INSTANCE = new ReceiveObjectHandler();

    private ReceiveObjectHandler() {
    }

    public static ReceiveObjectHandler getInstance() {
        return INSTANCE;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
        throws Exception {
        if (in.readableBytes() <= (INT_LENGTH<<1)) {
            return;
        }

        in.markReaderIndex();

        int msgHeader = in.readInt();//读取头魔数
        if (msgHeader != fastMqMagic) {
            ctx.channel().close();
        }

        int msgLen = in.readInt();//读取消息体长度
        if (msgLen <= 0) {
            return;
        }
        if (in.readableBytes() < msgLen) {
            in.resetReaderIndex();
            return;
        }

        /*
         * 开始解析store格式的byte[]数据,数据格式是 key长度+key的字节数组+value长度+value的字节数组
         * +topic name的长度+topic name的字节数组
         * 其中所有长度都是int类型，占4个字节
         * 数据有可能批量发过来，及batch模式生产，所以需要循环解析
         */
        while (in.isReadable()) {


            byte[] recMsgBytes = new byte[msgLen];


        }
        out.add(in.readBytes(recMsgBytes));
    }

    private void decodeEachKVData(ByteBuf in) {
        int keyLen = in.readInt();
        byte[] key = new byte[keyLen];
        in.readBytes(key);

        int valueLen = in.readInt();
        byte[] value = new byte[valueLen];
        in.readBytes(value);

        int topicNameLen = in.readInt();
        byte[] topicName = new byte[topicNameLen];
        in.readBytes(topicName);

        int partitionLen = in.readInt();
        byte[] partition = new byte[partitionLen];
        in.readBytes(partition);

        KeyMessage<byte[],byte[]> kv = new KeyMessage<>(key,value,topicName)
    }
}
