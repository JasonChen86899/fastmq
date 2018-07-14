package MQ.Cluster;

import java.util.List;
import java.util.concurrent.locks.LockSupport;

import com.github.zkclient.ZkClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by Jason Chen on 2016/9/5.
 */
@Configuration
public class ClusterManager extends Thread {

    /**
     * 建立连接，创建持久点进行消费服务器的注册
     */
    private static ZkClient zkClient;

    private static int childrenNum;

    //private static volatile List<String> currentChilds;

    //private static volatile List<String> MQServerNodes;

    private static void creatRootNode() {
        String Zkservers = "127.0.0.1:8880,127.0.0.1:8881,127.0.0.1:8881";
        zkClient = new ZkClient(Zkservers, 10000, 10000);
        System.out.println("connect ok!");
    }

    public static void creatChildNode(String data) {
        for (; ; ) {//自旋非阻塞
            try {
                zkClient.createPersistent("/Messageconsumer/" + childrenNum);
                zkClient.writeData("/Messageconsumer/" + childrenNum, data.getBytes());
                return;
            } catch (Exception e) {
                childrenNum++;
            }
        }
    }

    protected static void createMQServerNode(String ipAddress) {
        if (!zkClient.exists("/MQServers")) {
            zkClient.createPersistent("/MQServers");
        }
        zkClient.createPersistent("/MQServers/" + ipAddress);
        //zkClient.writeData("")
        //zkClient.writeData("/MQServers"+MQServerNodesNum,data.getBytes());
    }

    /**
     private static void SubscribeChildChanges(){
     IZkChildListener iZkChildListener = (String var1, List<String> var2) ->{
     //currentChilds = var2;
     };
     zkClient.subscribeChildChanges("/Messageconsumer",iZkChildListener);
     }**/

    /**
     * private static void SubscribeMQServerNodesChanges(){ IZkChildListener iZkChildListener = (String var1,
     * List<String> var2) -> { //MQServerNodes = var2; List<String> aliveMQServernodes =
     * ClusterManager.getMQServerNodes(); //if(aliveMQServernodes.stream().filter((a) -> a.equals()).findFirst();){
     * //return; //}
     *
     * String ipAddress = aliveMQServernodes.get(new Random(aliveMQServernodes.size()-1).nextInt()); ZMQ.Context context
     * = ZMQ.context(1); ZMQ.Socket socket = context.socket(ZMQ.PUSH); socket.connect(ipAddress);
     * socket.send("SetupFastMQ");
     *
     * }; zkClient.subscribeChildChanges("/MQServers",iZkChildListener); }
     */

    public static List getCurrentChilds() {
        return zkClient.getChildren("/Messageconsumer");
    }

    public static List getMQServerNodes() {
        return zkClient.getChildren("/MQServers");
    }

    @Bean
    public static ZkClient getZkClient() {
        if (zkClient != null) {
            return zkClient;
        } else {
            String Zkservers = "127.0.0.1:8880,127.0.0.1:8881,127.0.0.1:8881";
            zkClient = new ZkClient(Zkservers, 10000, 10000);
            System.out.println("connect ok!");
            return zkClient;
        }
    }

    public void run() {
        creatRootNode();//开启zk集群
        //SubscribeMQServerNodesChanges();//注册MQ服务器集群监听事件；跟ZK集群不一样
        //SubscribeChildChanges();//注册消息服务器监听事件
        for (; ; ) {
            while (Thread.currentThread().isInterrupted()) {
                //这里可以跟日志接口进行对接，记录监控进程的运行情况
            }
            LockSupport.park(Thread.currentThread());//阻塞当前的线程
            Thread.currentThread().interrupt();//写上中断标志位
        }
    }

}
