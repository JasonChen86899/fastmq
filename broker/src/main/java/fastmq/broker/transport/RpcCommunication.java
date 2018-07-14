package fastmq.broker.transport;

public interface RpcCommunication {

    void send(Object object);

    Object receive();

}
