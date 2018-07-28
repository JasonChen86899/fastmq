package fastmq.common.rpc;

import java.net.SocketAddress;

public interface RpcClient {

    void startClient(SocketAddress remoteAddr);

}
