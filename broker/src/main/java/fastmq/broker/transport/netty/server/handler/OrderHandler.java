package fastmq.broker.transport.netty.server.handler;

import fastmq.common.netty.dto.RpcObject;
import io.netty.channel.ChannelHandlerContext;

@FunctionalInterface
public interface OrderHandler {

    void handler(RpcObject object, ChannelHandlerContext ctx);
}
