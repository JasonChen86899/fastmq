package fastmq.broker.communication.netty;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

/**
 * Description: Author: Jason Chen Date: 2018/7/9
 */
public class NettyChannelInitializer extends ChannelInitializer<SocketChannel> {

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    ch.pipeline()
        .addLast()
  }
}
