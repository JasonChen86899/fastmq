package fastmq.common.netty.dto;

/**
 * Author: Jason Created in 2018/7/14
 */
public enum OrderEnum {

    ALLOCATE(1);

    private int orderId;

    OrderEnum(int i) {
        this.orderId = i;
    }

    public int getOrderId() {
        return this.orderId;
    }
}
