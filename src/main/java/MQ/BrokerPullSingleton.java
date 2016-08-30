package MQ;

import com.sun.corba.se.pept.broker.Broker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zeromq.ZMQ;

import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Created by I330347 on 2016/8/30.
 */
@Component
public class BrokerPullSingleton extends Thread {
    boolean falg;//线程停止标志
    private String tcpAddress;
    private ZMQ.Context context;
    private ZMQ.Socket puller;
    public BrokerPullSingleton(String adr){
        falg = true;
        this.tcpAddress = adr;
        context = ZMQ.context(1);
        puller = context.socket(ZMQ.PULL);
        puller.bind(tcpAddress);
    }
    public void run(){
        while(falg){
            /**
             * 此处需要进行 Protobuf 编解码，这里暂时以两个步骤替代
             */
            String  a = puller.recvStr();
            String  b = puller.recvStr();
            if(MQ.MessageQueueMap.getByName(a) == null) {
                MQ.MessageQueueMap.putByName(a);
                ZMQ.Socket pushToMQService = context.socket(ZMQ.PUSH);
                pushToMQService.bind("serviceaddress:端口");
                if(a.contains("Queue"))
                    pushToMQService.send("Queue");
                else
                    pushToMQService.send("Topic");
            }
            MessageQueueMap.getByName("a").add(b);
        }
    }
}
