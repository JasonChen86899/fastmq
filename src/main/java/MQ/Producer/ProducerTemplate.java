package MQ.Producer;

import MQ.Message.KeyMessage;
import MQ.Serialization.SerializationUtil;
import org.zeromq.ZMQ;
import zmq.Address;

import java.io.IOException;

/**
 * Created by Jason Chen on 2016/12/13.
 */
public abstract class ProducerTemplate {
    private ZMQ.Context context;
    private ZMQ.Socket socket;
    public ProducerTemplate(String mqPullSingletonIp){
        context = ZMQ.context(1);
        socket = context.socket(ZMQ.PUSH);
        socket.connect(mqPullSingletonIp);
    }
    public void sendMessageToMQServer(KeyMessage<String,Object> sendMessage){
        try {
            socket.send(SerializationUtil.serialize(sendMessage));
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}