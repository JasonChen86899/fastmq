package fastmq.client.consumer.transport.netty;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

/**
 * @Description:
 * @Author: c00420978
 * @Date: Created in 2018/7/26
 */
public class NettyChannelInitializer extends ChannelInitializer<SocketChannel>{

    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline()
            .addLast()
            .addLast()
    }
}
