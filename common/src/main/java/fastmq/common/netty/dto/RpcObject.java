package fastmq.common.netty.dto;

import java.util.List;

/**
 * Author: Jason Created in 2018/7/14
 */

public class RpcObject {

    private OrderEnum orderEnum;

    private boolean hasKeys;

    private List<String> keys;

    private List<Object> values;

    public void setOrderEnum(OrderEnum orderEnum) {
        this.orderEnum = orderEnum;
    }

    public void setHasKeys(boolean hasKeys) {
        this.hasKeys = hasKeys;
    }

    public void setKeys(List<String> keys) {
        this.keys = keys;
    }

    public void setValues(List<Object> values) {
        this.values = values;
    }

    public OrderEnum getOrderEnum() {
        return orderEnum;
    }

    public boolean isHasKeys() {
        return hasKeys;
    }

    public List<String> getKeys() {
        return keys;
    }

    public List<Object> getValues() {
        return values;
    }

}
