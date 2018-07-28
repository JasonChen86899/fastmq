package fastmq.common.rpc;

import java.net.SocketAddress;

public interface RpcServer {

    void startService(SocketAddress bindedAddr) throws Exception;

}
