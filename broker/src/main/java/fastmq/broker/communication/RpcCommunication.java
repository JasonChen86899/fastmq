package fastmq.broker.communication;

public interface RpcCommunication {

  void send(Object object);

  Object receive();

}
