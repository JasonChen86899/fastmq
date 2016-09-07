package MQ;

import com.github.zkclient.IZkChildListener;
import com.github.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by Jason Chen on 2016/9/5.
 */
public class ClusterManager extends Thread {
    /**
     * 中断标志位，1未中断，0表示中断，不用Thread.isInterrupt()的原因是由于JVM会自动清除中断标识位
     */
    private static AtomicInteger interruptFalg = new AtomicInteger(1);
    /**
     * 建立连接，创建持久点进行消费服务器的注册
     */
    private static ZkClient zkClient;

    private static int childrenNum;

    private static int MQServerNodesNum;

    private static volatile List<String> currentChilds;

    private static volatile List<String> MQServerNodes;

    private static void creatRootNode(){
        String Zkservers = "127.0.0.1:8880,127.0.0.1:8881,127.0.0.1:8881";
        zkClient = new ZkClient(Zkservers,10000,10000);
        System.out.println("connect ok!");
        String MessageConsumer = "MessageConsumer";
        String path = zkClient.create("/Messageconsumer",MessageConsumer.getBytes(), CreateMode.PERSISTENT);
    }

    public static void creatChildNode(String data){
        for (;;){//自旋非阻塞
            try{
                zkClient.createPersistent("/Messageconsumer/"+childrenNum);
                zkClient.writeData("/Messageconsumer/"+childrenNum,data.getBytes());
                return;
            }catch (Exception e){
                childrenNum++;
            }
        }
    }

    protected static void createMQServerNode(String data){
        for(;;){//自旋非阻塞
            try{
                zkClient.createPersistent("/MQServers"+MQServerNodesNum);
                zkClient.writeData("/MQServers"+MQServerNodesNum,data.getBytes());
            }catch (Exception e){
                MQServerNodesNum++;
            }
        }
    }

    private static void SubscribeChildChanges(){
        IZkChildListener iZkChildListener = (String var1, List<String> var2) ->{
            currentChilds = var2;
        };
        zkClient.subscribeChildChanges("/Messageconsumer",iZkChildListener);
    }

    private static void SubscribeMQServerNodesChanges(){
        IZkChildListener iZkChildListener = (String var1, List<String> var2) -> {
            MQServerNodes = var2;
        };
        zkClient.subscribeChildChanges("/MQServers",iZkChildListener);
    }

    public static List getCurrentChilds(){
        return currentChilds;
    }

    public static List getMQServerNodes(){
        return MQServerNodes;
    }

    public void run(){
        creatRootNode();//开启zk集群
        SubscribeMQServerNodesChanges();//注册MQ服务器集群监听事件；跟ZK集群不一样
        SubscribeChildChanges();//注册消息服务器监听事件
        for(;;){
            while (interruptFalg.intValue()==1){
                //这里可以跟日志接口进行对接，记录监控进程的运行情况
            }
            LockSupport.park(Thread.currentThread());//阻塞当前的线程
            Thread.currentThread().interrupt();//写上中断标志位
        }
    }

}
