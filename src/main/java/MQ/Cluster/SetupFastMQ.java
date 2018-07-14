package MQ.Cluster;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import MQ.MQService;
import MQ.ReceiveFromLeader;
import com.github.zkclient.IZkDataListener;
import com.github.zkclient.exception.ZkNoNodeException;
import org.springframework.beans.factory.annotation.Autowired;
import org.zeromq.ZMQ;

/**
 * Created by Jason Chen on 2016/9/8.
 */

/**
 * 每台MQServer需要启动这样一个线程，用来监听ZKClient的消息，以便PullSingleton线程消失，重新起一个线程
 */
public class SetupFastMQ extends Thread {

    @Autowired
    private MQService FastMQ;
    @Autowired
    private ReceiveFromLeader receiveFromPullSingleton;
    private ZMQ.Context context;
    private ZMQ.Socket setupFastMQLocalSocket;
    private String ipAddress;
    /**
     * 分布式锁，用来监控每台机器的SetFastMQ，只有一台机器获得锁，然后监听前一个节点的删除事件
     */
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    public SetupFastMQ(String adr) {
        this.ipAddress = adr;
        //this.context = ZMQ.context(1);
        //setupFastMQLocalSocket = context.socket(ZMQ.PULL);
        //setupFastMQLocalSocket.bind(adr);
    }

    public void run() {
        /*
        while (!setupFastMQLocalSocket.recvStr().equals("SetupFastMQ")) {
            //自旋，不过这里可以用Zk的分布式锁，其实性能呢~哈哈，过几天写一下ZK分布式锁
        }*/
        //分布式锁用来创建MQ
        registSetupService();
        //非Leader机器需要开启一个接受pull线程传送的数据的线程
        //ReceiveFromLeader receiveFromPullSingleton = new ReceiveFromLeader(ipAddress,ZMQ.PULL,true);
        receiveFromPullSingleton.start();
        while (!tryGetLock()) {
            //自旋
        }
        FastMQ.start();
        receiveFromPullSingleton.setFlag(false);
    }

    /**
     *
     */
    private void LoadBlance() {

    }

    /**
     * 注册Setup服务，用来争抢锁以便开启MQ服务
     */
    private void registSetupService() {
        ClusterManager.createMQServerNode(ipAddress);
    }

    /**
     * 尝试争抢锁
     */
    private boolean tryGetLock() {
        try {
            for (; ; ) {
                List<String> mqServer = ClusterManager.getMQServerNodes();
                //当等于0时表示可以取到锁
                int isGetTheLock = mqServer.indexOf(ipAddress);
                //网络闪断
                if (isGetTheLock < 0) {
                    throw new ZkNoNodeException("节点没有找到" + ipAddress);
                }
                if (isGetTheLock == 0) {
                    return true;
                }
                String preNodeipAddress = mqServer.get(isGetTheLock - 1);
                final CountDownLatch latch = new CountDownLatch(1);
                final IZkDataListener iZkDataListener = new IZkDataListener() {
                    public void handleDataChange(String dataPath, byte[] data) throws Exception {

                    }

                    public void handleDataDeleted(String dataPath) throws Exception {
                        latch.countDown();
                    }
                };
                ClusterManager.getZkClient().subscribeDataChanges("/MQServers/" + preNodeipAddress, iZkDataListener);
                latch.await();
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

}
