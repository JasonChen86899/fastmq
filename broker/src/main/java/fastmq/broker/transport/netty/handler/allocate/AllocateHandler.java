package fastmq.broker.transport.netty.handler.allocate;

import com.github.zkclient.ZkClient;
import fastmq.broker.partition.PartitionAllocate;
import fastmq.broker.transport.netty.handler.OrderHandler;
import fastmq.common.netty.dto.RpcObject;
import fastmq.common.netty.dto.allocate.AllocateRpcObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * Author: Jason Created in 2018/7/14
 */
public class AllocateHandler implements OrderHandler {

    private static Logger logger = LoggerFactory.getLogger(AllocateHandler.class);

    private static AllocateHandler INSTANCE;

    private ZkClient zkClient;

    private AllocateHandler(ZkClient zkClient) {
        this.zkClient = zkClient;
    }

    public static AllocateHandler getInstance(ZkClient zkClient) {
        if (Objects.isNull(INSTANCE)) {
            synchronized (AllocateHandler.class) {
                if (Objects.isNull(INSTANCE)) {
                    INSTANCE = new AllocateHandler(zkClient);
                }
            }
        }

        return INSTANCE;
    }

    @Override
    public void handler(RpcObject dto) {
        AllocateRpcObject aDto = (AllocateRpcObject) dto;

        try {
            PartitionAllocate
                .registTopicEvent(zkClient, aDto.getTopicName(), aDto.getPartitionNum());
        } catch (IOException e) {
            logger.error("Partition allocate error,topic_name: {},partition_num: {}",
                aDto.getTopicName(), aDto.getPartitionNum());
        }
    }
}
