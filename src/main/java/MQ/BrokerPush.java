package MQ;


import MQ.Consumer.ConsumerGroup;
import MQ.Consumer.ConsumerRule;
import MQ.Message.KeyMessage;
import MQ.Serialization.SerializationUtil;
import com.github.zkclient.ZkClient;
import org.apache.log4j.lf5.viewer.LogFactor5ErrorDialog;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;


/**
 * Created by Jason Chen on 2016/8/27.
 */
public class BrokerPush extends Thread {
    private String topicName;
    private String patition;
    private ArrayList<String> consumerIpAddress;
    private Queue mq;
    private boolean flag;
    private String tcp;
    private int type;
    private ZMQ.Context context;
    private ZMQ.Socket transfer;
    private ArrayList<ZMQ.Socket> pushSockets;
    /**
    public BrokerPush(String tcpAddress, int t, Queue messageQueue){//type 指的是ZMQ下面的传输方式
        this.mq = messageQueue;
        this.flag = true;
        this.tcp = tcpAddress;
        this.type = t;
        context = ZMQ.context(1);//可以分配两个线程给给这个context，主要针对两个线程一个分别是消费者，一个是生产者,当然也可以不这样做表示  启动两个在这样的线程，之间可以实行持久化
        this.transfer = context.socket(type);
        transfer.bind(tcp);
    }

    public BrokerPush(String tcpAddress, int t, String topicname, Queue messageQueue){//type 指的是ZMQ下面的传输方式
        this.topicName = topicname;
        this.mq = messageQueue;
        this.flag = true;
        this.tcp = tcpAddress;
        this.type = t;
        context = ZMQ.context(1);
        this.transfer = context.socket(type);
        transfer.bind(tcp);
    }
     **/

    public BrokerPush(ZkClient zkClient,String topic_patition, Queue messageQueue){
        String[] a = topic_patition.split("_");
        this.topicName = a[0];
        this.patition = a[1];
        this.flag = true;
        this.type = ZMQ.PUSH;
        context = ZMQ.context(1);
        this.consumerIpAddress = new ArrayList<>();
        sameTopicGroupPub(zkClient,topic_patition,topicName);
        //进行pushSockets的create然后进行发送
        consumerIpAddress.forEach((eachIp) -> {
            ZMQ.Socket transfer =context.socket(type);
            pushSockets.add(transfer);
            transfer.connect(eachIp);
        });
        this.mq = messageQueue;
}
    /**
    private boolean doSend(String sendData){
        try{
            return transfer.send(sendData);
        }catch (Throwable throwable) {
            return false;
        }
    }

    private boolean doSubSend(String sendDate,String topicName){
        try{
            return (transfer.sendMore(topicName) && transfer.send(sendDate));
        }catch (Throwable throwable){
            return false;
        }
    }

     **/
    public void run(){
        if(type == ZMQ.PUSH){
            while (flag){
                /**
                 * 这里暂时以打印为演示，方便调试
                 */
                //doSend((String)mq.getFirst());
                //System.out.println((String)mq.getFirst());

                /**
                 * main 代码块
                 */
                try {
                    sendToGroup(SerializationUtil.serialize((KeyMessage<Object,Object>)mq.poll()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        if(type == ZMQ.PUB){
            while (flag){
                /**
                 * 这里暂时以打印为演示，方便调试
                 */
                //doSubSend((String)mq.getFirst(),topicName);
                //System.out.println((String)mq.getFirst());
            }
        }
    }

    private void sameTopicGroupPub(ZkClient zkClient,String topic_patition,String topic){
        List<String> groupChildren = zkClient.getChildren("/Consumer/Topic/"+topic);
        /**
         * 注册监听事件，一旦消费者Group发生变化，需要立即停止线程然后重新获取collateMap
         */
        //List<String> groupChildren = zkClient.getChildren("/Consumer/Topic"+topicName);
        groupChildren.forEach(group ->
                zkClient.subscribeChildChanges("/Consumer/Group/"+group+"/ids", (s,list) -> {
                    flag = false;//停止发送数据报文
                    ConsumerRule.DefalutConsumerRule(topicName,new ConsumerGroup(zkClient,"/Consumer/Group/"+group+"/ids",group));
                }));
        groupChildren.forEach((group) -> {
            try {
                HashMap<String,List<String>> consumerip_List_topicPatition = (HashMap<String,List<String>>)SerializationUtil.deserialize(zkClient.readData("Consumer/Group/"+group+"/collateMap"));
                consumerIpAddress.add(consumerip_List_topicPatition.entrySet().stream().findFirst().filter((entry) ->
                   entry.getValue().contains(topic_patition)==true
                ).get().getKey());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private void sendToGroup(byte[] sendData){

        pushSockets.forEach((socket) -> socket.send(sendData));
    }
}
