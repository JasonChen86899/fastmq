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
        messageQueueMap.getByName(keyMessage.getTopic_name()).add(keyMessage);
    }
}
