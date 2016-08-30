package MQ;

/**
 * Created by I330347 on 2016/8/29.
 */


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ContextConfiguration;
import org.zeromq.ZMQ;

/**
 * 此客户端可以和Spring进行结合
 */
@Component
//@ContextConfiguration(locations = "classpath:applicationContext.xml")
public class MQService extends Thread {
    @Autowired
    private BrokerPullSingleton brokerPullSingleton;
    private String ServiceAddress;
    private ZMQ.Context context;
    private ZMQ.Socket transfer;
    public MQService(int type,String adr){
        this.ServiceAddress = adr;
        this.context = ZMQ.context(1);
        this.transfer = context.socket(type);
    }
    public void run(){
        brokerPullSingleton.start();
        String key =null;
        while (true) {
            if ((key = transfer.recvStr()).contains("Queue"))
                new BrokerPush(ServiceAddress, ZMQ.PUSH, MessageQueueMap.getByName(key)).start();
            if ((key = transfer.recvStr()).contains("Topic"))
                new BrokerPush(ServiceAddress, ZMQ.PUB, key, MessageQueueMap.getByName(key)).start();
        }
    }




}
