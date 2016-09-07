package MQ;

import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Created by Jason Chen on 2016/8/30.
 */
@Component
public class MessageQueueMap {
    private static HashMap<String,ConcurrentLinkedDeque> messageQueueMap = new HashMap<String, ConcurrentLinkedDeque>();
    //private static HashMap<String ,SynchronousQueue> queueMap = new HashMap<>();
    public static ConcurrentLinkedDeque getByName(String key){
        return messageQueueMap.get(key);
    }
    public static void putByName(String key){
        messageQueueMap.put(key,new ConcurrentLinkedDeque());
    }
    //public static SynchronousQueue getByQueueName(String queueName){
        //return queueMap.get(queueName);
    //}


}
