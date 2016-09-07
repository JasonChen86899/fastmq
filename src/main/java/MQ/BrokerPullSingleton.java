package MQ;

import org.zeromq.ZMQ;

/**
 * Created by Jason Chen on 2016/8/30.
 */

public class BrokerPullSingleton extends Thread {
    boolean falg;//线程停止标志
    private String tcpAddress;
    private String serviceAddress;
    private ZMQ.Context context;
    private ZMQ.Socket puller;
    public BrokerPullSingleton(String adr,String serveradr){
        falg = true;
        this.tcpAddress = adr;
        this.serviceAddress = serveradr;
        context = ZMQ.context(1);
        puller = context.socket(ZMQ.PULL);
        puller.bind(tcpAddress);
    }
    public void run(){
        while(falg){
            /**
             * 此处需要进行 Protobuf 编解码，这里暂时以两个步骤替代
             */
            String  a = "Queue";//puller.recvStr();
            String  b = "测试";//puller.recvStr();
            if(MQ.MessageQueueMap.getByName(a) == null) {
                MQ.MessageQueueMap.putByName(a);
                ZMQ.Socket pushToMQService = context.socket(ZMQ.PUSH);
                pushToMQService.connect(serviceAddress);
                if(a.contains("Queue"))
                    pushToMQService.send("Queue");
                else
                    pushToMQService.send("Topic");
            }
            MessageQueueMap.getByName(a).add(b);
        }
    }
}
