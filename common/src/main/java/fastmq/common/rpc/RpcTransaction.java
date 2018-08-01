package fastmq.common.rpc;

public interface RpcTransaction {

    boolean send(Object object,Object sender);

    Object receive(Object receiver);

}
