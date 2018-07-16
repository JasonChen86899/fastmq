package fastmq.common.rpc;

public interface RpcTransaction {

    void send(Object object);

    Object receive();

}
