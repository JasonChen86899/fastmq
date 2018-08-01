package fastmq.client.consumer.transport.netty;

import java.net.SocketAddress;
import java.util.Objects;
import java.util.concurrent.SynchronousQueue;

import fastmq.client.consumer.transport.netty.handler.ClientSendMsgHandler;
import fastmq.common.rpc.RpcTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description:
 * @Author: Goober Created in 2018/7/28
 */
public class ClientSender extends NettyRpcClient implements RpcTransaction {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientSender.class);

    /**
     * 客户端发送功能内的消息同步队列
     */
    private SynchronousQueue<Object> queue;

    /**
     * 客户端启动线程
     */
    private Thread rpcClientThread;

    /**
     * 服务器远程地址
     */
    private SocketAddress remoteAddr;

    public ClientSender(SocketAddress remoteAddr) {
        this.remoteAddr = remoteAddr;
    }

    public synchronized void startClient() {
        if (Objects.isNull(this.queue)) {
            ClientSendMsgHandler sendMsgHandler = new ClientSendMsgHandler();
            this.channel.pipeline().addLast(sendMsgHandler);
            this.queue = sendMsgHandler.getMsgQueue();
            this.rpcClientThread = new Thread(() -> super.startClient(this.remoteAddr));
            this.rpcClientThread.start();
        }
    }

    @Override
    public boolean send(Object msg, Object sender) {
        if (!rpcClientThread.isAlive()) {
            return false;
        }
        try {
            this.queue.put(msg);
            return true;
        } catch (InterruptedException e) {
            LOGGER.info("send operation has been interrupted.");
            return false;
        }
    }

    @Override
    public Object receive(Object receiver) {
        return null;
    }


}
