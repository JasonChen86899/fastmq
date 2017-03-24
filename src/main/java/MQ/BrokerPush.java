package MQ;


import MQ.Consumer.ConsumerGroup;
import MQ.Consumer.ConsumerRule;
import MQ.Message.KeyMessage;
import MQ.Serialization.SerializationUtil;
import MQ.Storage.MessageNumberRecords.SqlDBUtil;
import com.github.zkclient.ZkClient;
import org.zeromq.ZMQ;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.locks.LockSupport;


/**
 * Created by Jason Chen on 2016/8/27.
 */
public class BrokerPush extends Thread {
    private String topicName;
    private String patition;
    private Queue mq;
    private boolean flag;
    private boolean changeFlag;
    private String tcp;
    private int type;
    private ZMQ.Context context;
    private HashMap<String,ZMQ.Socket> newGroupPushSockets;
    private HashMap<String,ZMQ.Socket> groupPushSockets;//对应 组名-对应消费者socket的map
    private HashMap<String,String> groupConsumerIpAddress;//对应 组名-这个组内的消费者Ip地址的map
    private SqlDBUtil sqlDBUtil;
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

    public BrokerPush(ZkClient zkClient,String topic_patition, Queue messageQueue, SqlDBUtil sq) throws Exception {
        String[] a = topic_patition.split("_");
        if(a.length!=2) {
            this.topicName = a[0];
            this.patition = a[1];
            this.flag = true;
            this.type = ZMQ.PUSH;
            context = ZMQ.context(1);
            groupPushSockets = new HashMap<>();
            newGroupPushSockets = new HashMap<>();
            groupConsumerIpAddress = new HashMap<>();
            sameTopicGroupPub(zkClient, topic_patition, topicName);
            this.mq = messageQueue;
            this.changeFlag = false;
            this.sqlDBUtil = sq;
        }else
            throw new Exception("初始化错误");
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
                    if(changeFlag){
                     newGroupPushSockets.entrySet().forEach(entry ->
                         groupPushSockets.put(entry.getKey(),entry.getValue())
                     );
                    }
                    sendToGroup(SerializationUtil.serialize((KeyMessage<Object,Object>)mq.poll()));
                    sqlDBUtil.selectMessageNumByKeyAndUpdateNum(topicName+"_"+patition,"commited_num");
                } catch (Exception e) {
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
                    ConsumerRule.DefalutConsumerRule(topicName,new ConsumerGroup(zkClient,"/Consumer/Group/"+group+"/ids",group));
                    try {
                        HashMap<String,List<String>> consumerip_List_topicPatition = (HashMap<String,List<String>>)SerializationUtil.deserialize(zkClient.readData("Consumer/Group/"+group+"/collateMap"));
                        groupConsumerIpAddress.put(group,consumerip_List_topicPatition.entrySet().stream().findFirst().filter((entry) ->
                                entry.getValue().contains(topic_patition)==true
                        ).get().getKey());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    //进行相应的group的消费者的ip的改变
                    ZMQ.Socket changeSocket =context.socket(type);
                    changeSocket.connect(groupConsumerIpAddress.get(group));
                    newGroupPushSockets.put(group,changeSocket);
                    changeFlag = true;
                })
        );
        /**
         * 实际的操作初始化函数
         */
        groupChildren.forEach( group -> {
            try {
                HashMap<String,List<String>> consumerip_List_topicPatition = (HashMap<String,List<String>>)SerializationUtil.deserialize(zkClient.readData("Consumer/Group/"+group+"/collateMap"));
                groupConsumerIpAddress.put(group,consumerip_List_topicPatition.entrySet().stream().findFirst().filter((entry) ->
                        entry.getValue().contains(topic_patition)==true
                ).get().getKey());
            } catch (IOException e) {
                        e.printStackTrace();
            }
        });
        //进行pushSockets的create然后进行发送
        groupConsumerIpAddress.entrySet().forEach((eachEntry) -> {
            ZMQ.Socket transfer = context.socket(type);
            transfer.connect(eachEntry.getValue());
            groupPushSockets.put(eachEntry.getKey(),transfer);
        });
    }

    private void sendToGroup(byte[] sendData){
        //pushSockets.stream().filter((each) -> flag == true).forEach((socket) -> socket.send(sendData));
        groupPushSockets.entrySet().forEach((entry) -> {
            if(!entry.getValue().send(sendData)){
                while (!newGroupPushSockets.get(entry.getKey()).send(sendData)){
                    LockSupport.parkNanos(1000000000);
                }
            }
        });
    }
}
