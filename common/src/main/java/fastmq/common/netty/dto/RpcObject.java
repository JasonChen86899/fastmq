package fastmq.common.netty.dto;

import java.util.List;

/**
 * Author: Jason Created in 2018/7/14
 */

abstract public class RpcObject {

    private OrderEnum orderEnum;

    public OrderEnum getOrderEnum() {
        return orderEnum;
    }

    public void setOrderEnum(OrderEnum orderEnum) {
        this.orderEnum = orderEnum;
    }
}
