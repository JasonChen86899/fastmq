package fastmq.broker.transport.netty.client;

import fastmq.common.rpc.RpcClient;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;

/**
 * @Author: Goober Created in 2018/7/26
 */
public abstract class NettyRpcClient implements RpcClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyRpcClient.class);

    protected Channel channel;

    public void startClient(SocketAddress remoteAddr) {
        EpollEventLoopGroup workGroup = new EpollEventLoopGroup();
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
            .handler(new LoggingHandler(LogLevel.INFO))
            .handler(NettyChannelInitializer.getInstance());

        ChannelFuture channelFuture = bootstrap.connect(remoteAddr);
        try {
            channelFuture.sync();
            this.channel = channelFuture.channel();
            this.channel.closeFuture().sync();
        } catch (InterruptedException e) {
            LOGGER.info("netty client has been interrupted: {}", e);
        } finally {
            workGroup.shutdownGracefully();
        }
    }
}
