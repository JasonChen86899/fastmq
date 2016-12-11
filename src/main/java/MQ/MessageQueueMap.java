package MQ;

import MQ.Message.KeyMessage;
import com.github.zkclient.ZkClient;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Created by Jason Chen on 2016/8/30.
 */

/**
 * 每个broker Id 机器都要求生成这个消息Map，进行消息的异步发送，存储
 */
@Service
public class MessageQueueMap {
    private static ConcurrentHashMap<String,Queue<KeyMessage<Object,Object>>> messageQueueMap = new ConcurrentHashMap<String, Queue<KeyMessage<Object,Object>>>();
    public static Queue getByName(String topic_partition){
        if(messageQueueMap.get(topic_partition)==null)
            putByName(topic_partition);
        return messageQueueMap.get(topic_partition);
    }
    //优先队列保持队列的绝对有序性
    public static void putByName(String topic_patition){
        PriorityBlockingQueue<KeyMessage<Object,Object>> q;
        messageQueueMap.put(topic_patition,q = new PriorityBlockingQueue<>());

        /**
         * 每产生一个队列就产生对应的BrokerPush线程进行消息的推送
         */
        try {
            new BrokerPush(zkClient,topic_patition,q).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static ZkClient zkClient;

    @Autowired
    public void setZkClient(ZkClient zk){
        MessageQueueMap.zkClient = zk;
    }
}
