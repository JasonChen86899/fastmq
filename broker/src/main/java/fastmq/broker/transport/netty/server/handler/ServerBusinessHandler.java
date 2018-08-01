package fastmq.broker.transport.netty.server.handler;

import fastmq.broker.transport.netty.server.handler.allocate.AllocateHandler;
import fastmq.common.netty.dto.RpcObject;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;

/**
 * Author: Jason Chen Date: 2018/7/13
 */
@Sharable
public class ServerBusinessHandler implements ChannelInboundHandler {

    private static ServerBusinessHandler INSTANCE = new ServerBusinessHandler();

    private ServerBusinessHandler() {
    }

    public static ServerBusinessHandler getInstance() {
        return INSTANCE;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {

    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {

    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {

    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {

    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        RpcObject dto = (RpcObject) msg;

        switch (dto.getOrderEnum()) {
            case ALLOCATE:
                AllocateHandler.getInstance().handler(dto, ctx);
                break;
        }

    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {

    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {

    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {

    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

    }
}
