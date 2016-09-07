package MQ;

import com.github.zkclient.IZkChildListener;
import com.github.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by I330347 on 2016/9/5.
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

    private static volatile List<String> currentChilds;

    public static void creatRootNode(){
        String Zkservers = "127.0.0.1:8880,127.0.0.1:8881,127.0.0.1:8881";
        zkClient = new ZkClient(Zkservers,10000,10000);
        System.out.println("connect ok!");
        String MessageConsumer = "MessageConsumer";
        String path = zkClient.create("/Messageconsumer",MessageConsumer.getBytes(), CreateMode.PERSISTENT);
    }

    public static void creatChildNode(Object o){
        for (;;){
            try{
                zkClient.createPersistent("/Messageconsumer/"+childrenNum);
                return;
            }catch (Exception e){
                childrenNum++;
            }
        }
    }

    public static void SubscribeChildChanges(){
        IZkChildListener iZkChildListener = (String var1, List<String> var2) ->{
            currentChilds = var2;
        };
        zkClient.subscribeChildChanges("/Messageconsumer",iZkChildListener);
    }

    public static List getCurrentChilds(){
        return currentChilds;
    }

    public void run(){
        SubscribeChildChanges();
        try {
            while (interruptFalg.intValue()==1){

            }
        }catch (Exception e){
            System.out.println("ClusterManager was Interruoted");
        }
    }
    
}
