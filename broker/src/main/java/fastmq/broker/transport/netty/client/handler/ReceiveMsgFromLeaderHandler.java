package fastmq.broker.transport.netty.client.handler;

import java.util.List;

import fastmq.broker.message.KeyMessage;
import fastmq.broker.spring.SpringUtil;
import fastmq.broker.storage.FastDB;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;

/**
 * @Description:
 * @author: Goober
 * @date: 2018/8/2
 */
@Sharable
public class ReceiveMsgFromLeaderHandler implements ChannelInboundHandler {

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
        FastDB fastDB = SpringUtil.getBean(FastDB.class);
        if (msg instanceof List) {
            List<KeyMessage<String, Object>> recList = (List<KeyMessage<String, Object>>) msg;
            recList.forEach(e ->
                fastDB.putObject(e.getKey(), e.getValue(), e.getTopicName(), true)
            );
        }
        if (msg instanceof KeyMessage) {
            KeyMessage<String, Object> recMsg = (KeyMessage<String, Object>) msg;
            fastDB.putObject(recMsg.getKey(), recMsg.getValue(), recMsg.getTopicName(), true);
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
