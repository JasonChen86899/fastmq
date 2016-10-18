package MQ;

import MQ.Message.KeyMessage;
import org.springframework.stereotype.Component;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Created by Jason Chen on 2016/8/30.
 */

/**
 * 每个broker Id 机器都要求生成这个消息Map，进行消息的异步发送，存储
 */
@Component
public class MessageQueueMap {
    private static ConcurrentHashMap<String,Queue<KeyMessage<Object,Object>>> messageQueueMap = new ConcurrentHashMap<String, Queue<KeyMessage<Object,Object>>>();
    //private static HashMap<String ,SynchronousQueue> queueMap = new HashMap<>();
    public static Queue getByName(String topic_partition){
        return messageQueueMap.get(topic_partition);
    }
    public static void putByName(String topic_patition){
        messageQueueMap.put(topic_patition,new ConcurrentLinkedDeque());
    }
    //public static SynchronousQueue getByQueueName(String queueName){
        //return queueMap.get(queueName);
    //}


}
