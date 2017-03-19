package MQ;

import MQ.Message.KeyMessage;

/**
 * Created by Jason Chen on 2016/10/20.
 */
public class PutMessageToQueue extends Thread {
    private MessageQueueMap messageQueueMap;
    private KeyMessage keyMessage;
    public PutMessageToQueue(KeyMessage kvKeyMessage,MessageQueueMap messageQueueMap){
        this.keyMessage = kvKeyMessage;
        this.messageQueueMap = messageQueueMap;
    }
    public void run(){
        //根据 主题_分区号 进行阻塞队列分别
        messageQueueMap.getQueueByName(keyMessage.getTopic_name()+"_"+keyMessage.getPatition()).add(keyMessage);
    }
}
