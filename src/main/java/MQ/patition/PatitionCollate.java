package MQ.patition;

/**
 * Created by Jason Chen on 2016/10/2.
 */

import MQ.Message.KeyMessage;
import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;
import com.github.zkclient.IZkStateListener;
import com.github.zkclient.ZkClient;
import org.apache.zookeeper.Watcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 分区策略
 */
@Component
public class PatitionCollate {

    private static AtomicInteger expectedVersion = new AtomicInteger(0);

    private static ZkClient zkClient;

    public static ZkClient getZkClient() {
        return zkClient;
    }

    /**
     * 注入静态变量，需要定义getter，setter然后在 1.xml文件里面进行定义 2. 如下面的做法（stackoverflow上面分享的做法）
     * @param zkClient
     */
    @Autowired
    public static void setZkClient(ZkClient zkClient) {
        PatitionCollate.zkClient = zkClient;
    }

    /**
     * 注册 topic 事件，进行主题 分区数量的设定
     * @param topic_name
     * @param paitionNum 由于 int（四个字节） 转 byte （1个字节），会丢失钱3个字节，所以取值范围为 1-255
     * @throws IOException
     */
    public static void registTopicEvent(final String topic_name, final int paitionNum) throws IOException {
        zkClient.createEphemeral("/"+topic_name);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        Hessian2Output hessian2Output = new Hessian2Output();
        hessian2Output.init(byteArrayOutputStream);
        List<String> ipList= zkClient.getChildren("/MQServers");
        if(ipList.size()>paitionNum)
            ipList = ipList.subList(0,paitionNum-1);
        hessian2Output.writeObject(ipList);
        byte[] bytes = byteArrayOutputStream.toByteArray();
        zkClient.createEphemeral("/MessageData/"+topic_name);
        zkClient.writeData("/MessageData/"+topic_name,bytes);
        zkClient.createEphemeral("/PatitionNum/"+topic_name);
        zkClient.writeData("/PatitionNum/"+topic_name,new byte[]{(byte)paitionNum});
        if(zkClient.exists("/PatitionInfo"))
            zkClient.createPersistent("/PatitionInfo");
        hessian2Output.flush();
        ByteArrayOutputStream byteArray_PatitionIndfo = new ByteArrayOutputStream();
        hessian2Output.writeObject(new HashMap<String,ArrayList<Integer>>());
        zkClient.writeData("/PatitionInfo",byteArray_PatitionIndfo.toByteArray());
        final IZkStateListener iZkStateListener = new IZkStateListener() {
            @Override
            public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                Hessian2Output hessian2Output = new Hessian2Output();
                hessian2Output.init(byteArrayOutputStream);
                List<String> ipList= zkClient.getChildren("/MQServers");
                if(ipList.size()>paitionNum)
                    ipList = ipList.subList(0,paitionNum-1);
                hessian2Output.writeObject(ipList);
                byte[] bytes = byteArrayOutputStream.toByteArray();
                zkClient.writeData("/MessageData/"+topic_name,bytes);
                //分为两种情况：
                // 1.MQ宕机；2.MQ拓展
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(zkClient.readData("/PatitionInfo"));
                Hessian2Input hessian2Input = new Hessian2Input();
                hessian2Input.init(byteArrayInputStream);
                HashMap<String,HashMap<String,ArrayList<Integer>>> map = (HashMap<String,HashMap<String,ArrayList<Integer>>>)hessian2Input.readObject();
                final List<String> ip_list = ipList;
                if(map.keySet().size()>ipList.size()){
                    List<String> downIp = map.keySet().stream().filter(
                            ipStrig -> !ip_list.contains(ipStrig)
                    ).collect(Collectors.toList());
                    /**
                     * 取出宕机的ip，将其上的指定topic的分区号集合拿出，进行根据i编号（i，如下面的for循环指的是index）
                     * hash取模依次分配，再记录，最后写入zk
                     */
                    downIp.stream().forEach(ipString -> {
                        ArrayList<Integer> list_patitions = map.get(ipString).get(topic_name);
                        for(int i=0; i<list_patitions.size(); i++){
                            int brokerSize = ip_list.size();
                            map.get(ip_list.get(i%(brokerSize-1))).get(topic_name).add(list_patitions.get(i));
                        }
                    });
                    hessian2Output.flush();
                    ByteArrayOutputStream byte_write_back_to_zk = new ByteArrayOutputStream();
                    hessian2Output.init(byte_write_back_to_zk);
                    hessian2Output.writeObject(map);
                    zkClient.writeData("/PatitionInfo",byte_write_back_to_zk.toByteArray());
                }
            }

            @Override
            public void handleNewSession() throws Exception {

            }
        };
        zkClient.subscribeStateChanges(iZkStateListener);
        for(int i=1; i<=paitionNum; i++){
            int mod = i%ipList.size();
            String ipAddress = ipList.get(mod-1);
            //因为是分布式的机器，所以需要zkClient writeData乐观锁，所以这里需要写上for(;;)
            Hessian2Input hessian2Input = new Hessian2Input();
            for (;;){
                try{
                    byte[] ipAddressInfo = zkClient.readData("/PatitionInfo");
                    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(ipAddressInfo);
                    hessian2Input.init(byteArrayInputStream);
                    HashMap<String,HashMap<String,ArrayList<Integer>>> Map_ip_patitionlist = (HashMap<String,HashMap<String,ArrayList<Integer>>>)hessian2Input.readObject();
                    if(Map_ip_patitionlist.get(ipAddress)==null) {
                        HashMap<String,ArrayList<Integer>> map = new HashMap<>();
                        ArrayList<Integer> arrayList = new ArrayList<Integer>();
                        arrayList.add(i);
                        map.put(topic_name,arrayList);
                        Map_ip_patitionlist.put(ipAddress,map);
                    }else {
                        if(Map_ip_patitionlist.get(ipAddress).get(topic_name)==null){
                            ArrayList<Integer> arrayList = new ArrayList<Integer>();
                            arrayList.add(i);
                            Map_ip_patitionlist.get(ipAddress).put(topic_name,arrayList);
                        }else
                            Map_ip_patitionlist.get(ipAddress).get(topic_name).add(i);
                    }
                    hessian2Output.flush();
                    hessian2Output.writeObject(Map_ip_patitionlist);
                    zkClient.writeData("/PatitionInfo",byteArrayOutputStream.toByteArray(),expectedVersion.intValue());
                    break;
                }catch (Exception e){
                    expectedVersion.incrementAndGet();//等同于 ++ 操作
                }
            }

        }
    }

    /**
     * 负载均衡LoadBalance，对topic进行分组的本质原因,这里跟kafka不一样，采用的是hashcode取模，
     * （hash一致性算法这里没有用主要是应用场景的不同）
     *
     * 这里是一个有别于kafka patition 的负载均衡和备份机制，也许就是一点点创新的地方吧
     * 数据持久化方案则需要采取不同的方案，但是则需要数据的主从备份，甚至可以将数据持久化单独独立出来，这样性能就完全需要
     * 闪存来保证
     * -->单线程先从客户端拉数据，然后在各个broker上进行分发存储，保持数据的一致，然后各个broker从自己闪存中取数据，
     * topic队列的 keyMessage 还是由 （这个函数去分区，传输给相应的broker 机器）
     * @param topic_name
     * @param keyMessage
     * @return
     * @throws IOException
     */
    public static String setTopicPatiton(final String topic_name,KeyMessage<Object,Object> keyMessage) throws IOException {
        byte[] byte_map = zkClient.readData("/PatitionInfo");
        byte[] byte_broker_num = zkClient.readData("/PatitionNum/"+topic_name);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byte_map);
        Hessian2Input hessian2Input = new Hessian2Input();
        hessian2Input.init(byteArrayInputStream);
        HashMap<String,HashMap<String,ArrayList<Integer>>> map = (HashMap<String,HashMap<String,ArrayList<Integer>>>)hessian2Input.readObject();
        ByteArrayInputStream byteArrayInputStream1 = new ByteArrayInputStream(byte_broker_num);
        hessian2Input.init(byteArrayInputStream1);
        int patition = (int)hessian2Input.readObject();
        //找出对应分区所在的broker的ip地址
        String target_ip = map.entrySet().stream().filter(a ->
            a.getValue().get(topic_name).stream().filter(b -> b.intValue()==(Hash_Key(keyMessage)%patition)).count()==1
        ).findFirst().get().getKey();
        return target_ip;
    }

    public static int Hash_Key(KeyMessage<Object,Object> keyMessage){
        //key的处理方法
        return 1;
    }

}
