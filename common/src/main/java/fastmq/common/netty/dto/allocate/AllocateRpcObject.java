package fastmq.common.netty.dto.allocate;

import fastmq.common.netty.dto.RpcObject;

/**
 * @Description:
 * @author: Jason
 * @date: 2018/7/15
 */
public class AllocateRpcObject extends RpcObject {

    private String topicName;

    private int partitionNum;

    public String getTopicName() {
        return this.topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getPartitionNum() {
        return this.partitionNum;
    }

    public void setPartitionNum(int partitionNum) {
        this.partitionNum = partitionNum;
    }
}
