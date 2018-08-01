package fastmq.client.consumer.transport.netty.handler;

import java.net.SocketAddress;
import java.util.concurrent.SynchronousQueue;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPromise;

/**
 * @Description:
 * @Author: Goober Created in 2018/7/28
 */
@Sharable
public class ClientSendMsgHandler implements ChannelOutboundHandler {

    private SynchronousQueue<Object> msgQueue = new SynchronousQueue<>();

    public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise promise) throws Exception {

    }

    public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
        ChannelPromise promise) throws Exception {

    }

    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {

    }

    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {

    }

    public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {

    }

    public void read(ChannelHandlerContext ctx) throws Exception {

    }

    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        Object sendMsg = msgQueue.take();
        ctx.writeAndFlush(sendMsg, promise);
        promise.sync();
    }

    public void flush(ChannelHandlerContext ctx) throws Exception {

    }

    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {

    }

    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {

    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

    }

    public SynchronousQueue<Object> getMsgQueue() {
        return this.msgQueue;
    }
}
