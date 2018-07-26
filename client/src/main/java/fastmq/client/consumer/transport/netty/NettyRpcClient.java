package fastmq.client.consumer.transport.netty;

import fastmq.common.rpc.RpcClient;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

/**
 * @Author: c00420978 Created in 2018/7/26
 */
public class NettyRpcClient implements RpcClient {

    private static int workThreads = Runtime.getRuntime().availableProcessors() << 2;

    public void startClient() {
        EpollEventLoopGroup workGroup = new EpollEventLoopGroup(workThreads);
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workGroup)
            .channel(SocketChannel.class)
            .option()
            .handler(new LoggingHandler(LogLevel.INFO))
            .handler()
    }
}
