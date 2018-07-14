package fastmq.broker.transport.netty.handler;

import java.util.List;

import fastmq.broker.serialization.SerializationUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;

/**
 * Author: Jason Chen Date: 2018/7/10
 */
@Sharable
public class RpcObjectHandler extends ByteToMessageCodec<Object> {

    private static final int fastMqMagic = 0xfacd89;

    private static RpcObjectHandler INSTANCE = new RpcObjectHandler();

    private RpcObjectHandler() {
    }

    public static RpcObjectHandler getInstance() {
        return INSTANCE;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        out.writeInt(fastMqMagic);

        byte[] dataBytes = SerializationUtil.serialize(msg);
        out.writeInt(dataBytes.length);
        out.writeBytes(dataBytes);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        int fastMqHeader = in.readInt();
        if (fastMqHeader != fastMqMagic) {
            ctx.close();
        }

        int dataLength = in.readInt();
        if (in.readableBytes() < dataLength) {
            return;
        }

        byte[] dataBytes = new byte[dataLength];
        in.readBytes(dataBytes);

        out.add(SerializationUtil.deserialize(dataBytes));
    }
}
