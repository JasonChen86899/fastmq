package fastmq.broker.transport.netty.client;

import fastmq.broker.transport.netty.client.handler.ReceiveObjectHandler;
import fastmq.common.netty.handler.RpcObjectHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

/**
 * @Description:
 * @Author: Goober Created in 2018/7/26
 */
public class NettyChannelInitializer extends ChannelInitializer<SocketChannel> {

    private static final NettyChannelInitializer INSTANCE = new NettyChannelInitializer();

    private NettyChannelInitializer() {
    }

    static NettyChannelInitializer getInstance() {
        return INSTANCE;
    }

    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline()
            .addLast(ReceiveObjectHandler.getInstance())
        .addLast();
    }
}
