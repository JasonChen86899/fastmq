package MQ;

import MQ.Message.KeyMessage;
import MQ.Storage.MessageStorageStructure;
import MQ.Storage.MessageNumberRecords.SqlDBUtil;
import com.github.zkclient.ZkClient;
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
    private static SqlDBUtil sqlDBUtil;
    private static MessageStorageStructure messageStorageStructure;
    @Autowired
    public void setSqlDBUtil(SqlDBUtil db){
        sqlDBUtil = db;
    }
    @Autowired
    public void setMessageStorageStructure(MessageStorageStructure mss){
        messageStorageStructure = mss;
    }

    private static ConcurrentHashMap<String,Queue<KeyMessage<String,Object>>> messageQueueMap = new ConcurrentHashMap<String, Queue<KeyMessage<String,Object>>>();
    public static Queue getQueueByName(String topic_partition){
        if(messageQueueMap.get(topic_partition)==null)
            creatQueueByname(topic_partition);
        return messageQueueMap.get(topic_partition);
    }
    //优先队列保持队列的绝对有序性
    public static void creatQueueByname(String topic_patition){
        PriorityBlockingQueue<KeyMessage<String,Object>> q;
        messageQueueMap.put(topic_patition,q = new PriorityBlockingQueue<>());
        /**
         * 在创建队列的时候进行检查，是否是进过宕机或者扩容，有的话需要进行下面的操作
         */
        try{
            messageStorageStructure.getMessageAndPutIntoQueue(topic_patition,q);
        }catch (Exception e){
            if(!(e instanceof NullPointerException))//如果是初始分配非宕机或扩容的恢复则绕过
                e.printStackTrace();
        }

        /**
         * 每产生一个队列就产生对应的BrokerPush线程进行消息的推送
         */
        try {
            new BrokerPush(zkClient,topic_patition,q,sqlDBUtil).start();
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
