package fastmq.broker;

/**
 * Created by Jason Chen on 2016/8/29.
 */


import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.beans.factory.annotation.Autowired;
import org.zeromq.ZMQ;

/**
 * 此客户端可以和Spring进行结合
 */

//@ContextConfiguration(locations = "classpath:applicationContext.xml")
public class MQService extends Thread {

  private static AtomicInteger atomicInteger = new AtomicInteger(1);
  @Autowired
  private BrokerPullSingleton brokerPullSingleton;
  private String ServiceAddress;
  private ZMQ.Context context;
  private ZMQ.Socket transfer;

  public MQService(String adr) {
    this.ServiceAddress = adr;
    this.context = ZMQ.context(1);
    this.transfer = context.socket(ZMQ.PULL);
    transfer.bind(ServiceAddress);
  }

  public void run() {
    System.out.println("创建了pull单例线程");
    brokerPullSingleton.start();
    /**
     String key =null;
     String pushAddress = handleTcpAddress();
     while (true) {
     if ((key = transfer.recvStr()).contains("Queue")) {
     System.out.println("创建了队列push线程");
     try {
     new BrokerPush(pushAddress, ZMQ.PUSH, MessageQueueMap.getByName(key)).start();
     } catch (Exception e) {
     e.printStackTrace();
     }
     }
     if ((key = transfer.recvStr()).contains("Topic")) {
     System.out.println("创建了主题push线程");
     try {
     new BrokerPush(pushAddress, ZMQ.PUB, key, MessageQueueMap.getByName(key)).start();
     } catch (Exception e) {
     e.printStackTrace();
     }
     }
     }**/
  }

  private String handleTcpAddress() {
    String pre = ServiceAddress.substring(0, ServiceAddress.length() - 4);
    String post = "888" + atomicInteger.getAndIncrement();
    return pre + post;
  }

}
