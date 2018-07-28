package fastmq.common.rpc;

/**
 * @Description:
 * @Author: Goober Created in 2018/7/28
 */
public interface RpcSender {

    void send(Object msg);
}
