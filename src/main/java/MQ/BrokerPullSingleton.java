package MQ;

import MQ.Message.KeyMessage;
import MQ.Serialization.SerializationUtil;
import MQ.Storage.MessageStorageStructure;
import MQ.patition.PatitionCollate;
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
    boolean flag;//线程停止标志
    boolean synStorage;//刷盘标志，默认是true
    @Autowired
    private MessageStorageStructure messageStorage;
    @Autowired
    private MessageQueueMap messageQueueMap;
    private String tcpAddress;
    private String serviceAddress;
    private ZMQ.Context context;
    private ZMQ.Socket puller;
    public BrokerPullSingleton(String adr,String serveradr){
        flag = true;
        synStorage = true;//默认是true
        this.tcpAddress = adr;
        this.serviceAddress = serveradr;
        context = ZMQ.context(1);
        puller = context.socket(ZMQ.PULL);
        puller.bind(tcpAddress);
    }

    public BrokerPullSingleton(String adr,String serveradr,boolean synStorageFlag){
        flag = true;
        synStorage = synStorageFlag;
        synStorage = true;
        this.tcpAddress = adr;
        this.serviceAddress = serveradr;
        context = ZMQ.context(1);
        puller = context.socket(ZMQ.PULL);
        puller.bind(tcpAddress);
    }
    public void run(){
        while(flag){
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
                if (synStorage == true) {
                    //开始将信息进行存储,同步刷盘
                    if (messageStorage.sycSaveMessage(msg)) {
                        //开启一个线程，向其他MQ机器传输消息
                        new Thread() {
                            public void run() {
                                ZMQ.Socket pushToMQService = context.socket(ZMQ.PUSH);
                                int flag_message = 1;
                                while (flag_message == 1) {
                                    try {
                                        List<String> ipList = zkClient.getChildren("/MQServers");
                                        for (int i = 0; i < ipList.size(); i++) {
                                            pushToMQService.connect(ipList.get(0));
                                            pushToMQService.send(revice_bytes);
                                        }
                                        //以上一旦发生错误就会不进行置0，然后继续进行从zk拿信息传输
                                        flag_message = 0;
                                    } catch (Exception e) {
                                    }
                                }
                            }
                        }.start();
                        try {
                            if(tcpAddress == PatitionCollate.getIpAddressByTopicPatition(msg.getTopic_name(),msg)){
                                new PutMessageToQueue(msg,messageQueueMap).start();
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
                if(synStorage == false) {
                    //异步刷盘,这里的异步刷盘只是DB的异步，真正的异步需要别的方案，要好好想想
                    if (messageStorage.asycSaveMessage(msg)) {
                        //开启一个线程，向其他MQ机器传输消息
                        new Thread() {
                            public void run() {
                                ZMQ.Socket pushToMQService = context.socket(ZMQ.PUSH);
                                int flag_message = 1;
                                while (flag_message == 1) {
                                    try {
                                        List<String> ipList = zkClient.getChildren("/MQServers");
                                        for (int i = 0; i < ipList.size(); i++) {
                                            pushToMQService.connect(ipList.get(0));
                                            pushToMQService.send(revice_bytes);
                                        }
                                        //以上一旦发生错误就会不进行置0，然后继续进行从zk拿信息传输
                                        flag_message = 0;
                                    } catch (Exception e) {
                                    }
                                }
                            }
                        }.start();
                        try {
                            if (tcpAddress == PatitionCollate.getIpAddressByTopicPatition(msg.getTopic_name(), msg)) {
                                new PutMessageToQueue(msg, messageQueueMap).start();
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }
}
