package MQ;

import MQ.Message.KeyMessage;
import MQ.Serialization.SerializationUtil;
import com.github.zkclient.ZkClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.util.List;

/**
 * Created by Jason Chen on 2016/8/30.
 */

public class BrokerPullSingleton extends Thread {
    @Autowired
    private ZkClient zkClient;
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
            //1KB的信息量,用来基本的信息传输，这个值是暂时的设定
            byte[] revice_bytes = new byte[1024];
            puller.recv(revice_bytes,0,1024,1);
            KeyMessage<String,Object> msg;
            try {
                msg = (KeyMessage<String,Object>)SerializationUtil.deserialize(revice_bytes);
            } catch (IOException e) {
                msg = null;
            }
            /*
            //String  a = "Queue";//puller.recvStr();
            //String  b = "测试";//puller.recvStr();
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
            */
            if(msg!=null) {
                //开启一个线程，向其他MQ机器传输消息
                new Thread() {
                    public void run() {
                        ZMQ.Socket pushToMQService = context.socket(ZMQ.PUSH);
                        int flag = 1;
                        while (flag == 1) {
                            try {
                                List<String> ipList = zkClient.getChildren("/MQServers");
                                for (int i = 0; i < ipList.size(); i++) {
                                    pushToMQService.connect(ipList.get(0));
                                    pushToMQService.send(revice_bytes);
                                }
                                //以上一旦发生错误就会不进行置0，然后继续进行从zk拿信息传输
                                flag = 0;
                            } catch (Exception e) {
                            }
                        }
                    }
                }.start();
            }
            //开始讲信息进行存储
            
        }
    }
}
