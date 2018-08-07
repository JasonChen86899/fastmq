package fastmq.broker.transport.netty.client.handler;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

/**
 * @Description:
 * @Author: Goober Created in 2018/8/7
 */
@Sharable
public class ReceiveObjectHandler extends ByteToMessageDecoder {

    private static final int fastMqMagic = 0xfacd89;

    private static final ReceiveObjectHandler INSTANCE = new ReceiveObjectHandler();

    private ReceiveObjectHandler() {
    }

    public static ReceiveObjectHandler getInstance() {
        return INSTANCE;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < 4) {
            return;
        }

        in.markReaderIndex();

        if (in.readableBytes() == 4) {
            int msgHeader = in.readInt();
            if (msgHeader != fastMqMagic) {
                ctx.channel().close();
            }
            return;
        }
        if (in.readableBytes() < 4) {
            return;
        }
        int msgLen = in.readInt();
        if (msgLen <= 0) {
            return;
        }
        if (in.readableBytes() < msgLen) {
            in.resetReaderIndex();
            return;
        }

        /*
         * 开始解析store格式的byte[]数据,数据格式是 key长度+key的字节数组+value长度+value的字节数组
         * +topic name的长度+topic name的字节数组+partition的长度+partition的字节数组
         * 其中所有长度都是int类型，占4个字节
         */
        int keyLen = in.readInt();
        if(in.readableBytes() < keyLen){
            in.resetReaderIndex();

        }

        byte[] recMsgBytes = new byte[msgLen];

        out.add(in.readBytes(recMsgBytes));
    }
}
