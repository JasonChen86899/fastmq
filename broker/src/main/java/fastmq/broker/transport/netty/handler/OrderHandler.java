package fastmq.broker.transport.netty.handler;

import fastmq.common.netty.dto.RpcObject;
import io.netty.channel.ChannelHandlerContext;

@FunctionalInterface
public interface OrderHandler {

    void handler(RpcObject object, ChannelHandlerContext ctx);
}
