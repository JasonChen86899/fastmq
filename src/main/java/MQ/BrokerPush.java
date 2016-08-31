package MQ;


import org.zeromq.ZMQ;

import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Created by I330347 on 2016/8/27.
 */
public class BrokerPush extends Thread {
    private String topicName;
    private ConcurrentLinkedDeque mq;
    private boolean flag;
    private String tcp;
    private int type;
    private ZMQ.Context context;
    private ZMQ.Socket transfer;
    public BrokerPush(String tcpAddress, int t, ConcurrentLinkedDeque messageQueue){//type 指的是ZMQ下面的传输方式
        this.mq = messageQueue;
        this.flag = true;
        this.tcp = tcpAddress;
        this.type = t;
        context = ZMQ.context(1);//可以分配两个线程给给这个context，主要针对两个线程一个分别是消费者，一个是生产者,当然也可以不这样做表示  启动两个在这样的线程，之间可以实行持久化
        this.transfer = context.socket(type);
        transfer.bind(tcp);
    }

    public BrokerPush(String tcpAddress, int t, String topicname, ConcurrentLinkedDeque messageQueue){//type 指的是ZMQ下面的传输方式
        this.topicName = topicname;
        this.mq = messageQueue;
        this.flag = true;
        this.tcp = tcpAddress;
        this.type = t;
        context = ZMQ.context(1);//可以分配两个线程给给这个context，主要针对两个线程一个分别是消费者，一个是生产者,当然也可以不这样做表示  启动两个在这样的线程，之间可以实行持久化
        this.transfer = context.socket(type);
        transfer.bind(tcp);
    }

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

    public void run(){
        if(type == ZMQ.PUSH){
            while (flag){
                /**
                 * 这里暂时以打印为演示，方便调试
                 */
                //doSend((String)mq.getFirst());
                System.out.println((String)mq.getFirst());
            }
        }
        if(type == ZMQ.PUB){
            while (flag){
                /**
                 * 这里暂时以打印为演示，方便调试
                 */
                //doSubSend((String)mq.getFirst(),topicName);
                System.out.println((String)mq.getFirst());
            }
        }
    }

}
