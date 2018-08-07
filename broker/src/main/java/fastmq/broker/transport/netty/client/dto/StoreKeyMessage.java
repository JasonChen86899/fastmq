package fastmq.broker.transport.netty.client.dto;

import fastmq.common.netty.dto.RpcObject;

/**
 * @Description:
 * @Author: Goober Created in 2018/8/7
 */
public class StoreKeyMessage extends RpcObject {
    private byte[] key;

    private byte[] value;

    private byte[] topicName;

    private byte[] partition;

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public byte[] getTopicName() {
        return topicName;
    }

    public void setTopicName(byte[] topicName) {
        this.topicName = topicName;
    }

    public byte[] getPartition() {
        return partition;
    }

    public void setPartition(byte[] partition) {
        this.partition = partition;
    }
}
