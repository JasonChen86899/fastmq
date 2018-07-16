package fastmq.broker.transport.netty.handler;

import fastmq.common.netty.dto.RpcObject;

@FunctionalInterface
public interface OrderHandler {

  void handler(RpcObject object);
}
