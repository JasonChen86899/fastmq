package fastmq.client.consumer.transport.netty;

import java.net.SocketAddress;
import java.util.Objects;
import java.util.concurrent.SynchronousQueue;

import fastmq.client.consumer.transport.netty.handler.ClientSendMsgHandler;
import fastmq.common.rpc.RpcTransaction;

/**
 * @Description:
 * @Author: Goober Created in 2018/7/28
 */
public class ClientSender extends NettyRpcClient implements RpcTransaction {

    private SynchronousQueue<Object> queue;

    private Thread rpcClientThread;

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
    public void send(Object msg, Object sender) {
        try {
            this.queue.put(msg);
        } catch (InterruptedException e) {

        }
    }

    @Override
    public Object receive(Object receiver) {
        return null;
    }


}
