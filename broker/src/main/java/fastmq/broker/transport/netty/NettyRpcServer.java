package fastmq.broker.transport.netty;

import fastmq.broker.transport.RpcServer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

/**
 * Author: Jason Chen Date: 2018/7/8
 */
public abstract class NettyRpcServer implements RpcServer {

    private static int bossThreads = 1;
    private static int workThreads = Runtime.getRuntime().availableProcessors() << 2;

    private boolean ifServerOn;

    @Override
    public void startService() {
        //此处系统由于是在Linux执行所以选择Epoll，没有选择通用NioEventLoopGroup
        EpollEventLoopGroup bossGroup = new EpollEventLoopGroup(1);
        EpollEventLoopGroup worksGroup = new EpollEventLoopGroup(workThreads);

        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(bossGroup, worksGroup)
            .channel(EpollServerSocketChannel.class)
            .option(ChannelOption.SO_BACKLOG, 100)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 100)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .handler(new LoggingHandler(LogLevel.INFO))
            .childHandler(NettyChannelInitializer.getInstance());
    }

    @Override
    public void send(Object object) {

    }

    @Override
    public Object receive() {
        return null;
    }

}
