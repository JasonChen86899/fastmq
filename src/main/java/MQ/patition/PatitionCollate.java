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
import java.util.List;

/**
 * 分区策略
 */
@Component
public class PatitionCollate {

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
     * 暂时想不出这个有什么用
     * @param topic_name
     * @param paitionNum
     * @throws IOException
     */
    public static void subcriTopicEvent(final String topic_name, final int paitionNum) throws IOException {
        zkClient.createEphemeral("/"+topic_name);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        Hessian2Output hessian2Output = new Hessian2Output(byteArrayOutputStream);
        List<String> ipList= zkClient.getChildren("/MQServers");
        if(ipList.size()>paitionNum)
            ipList = ipList.subList(0,paitionNum-1);
        hessian2Output.writeObject(ipList);
        byte[] bytes = byteArrayOutputStream.toByteArray();
        zkClient.writeData("/"+topic_name,bytes);
        final IZkStateListener iZkStateListener = new IZkStateListener() {
            @Override
            public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                Hessian2Output hessian2Output = new Hessian2Output(byteArrayOutputStream);
                List<String> ipList= zkClient.getChildren("/MQServers");
                if(ipList.size()>paitionNum)
                    ipList = ipList.subList(0,paitionNum-1);
                hessian2Output.writeObject(ipList);
                byte[] bytes = byteArrayOutputStream.toByteArray();
                zkClient.writeData("/MessageData"+topic_name,bytes);
            }

            @Override
            public void handleNewSession() throws Exception {

            }
        };
        zkClient.subscribeStateChanges(iZkStateListener);
    }

    /**
     * 负载均衡LoadBalance，对topic进行分组的本质原因,这里个kafka不一样，采用的是hashcode取模，（hash一致性算法这里没有用主要是应用场景的不同）
     * @param keyMessage
     */
    public static String setTopicPatiton(String topic_name,KeyMessage<Object,Object> keyMessage) throws IOException {
        byte[] bytes = zkClient.readData("//MessageData"+topic_name);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        Hessian2Input hessian2Input = new Hessian2Input(byteArrayInputStream);
        List<String> ipList = (List)hessian2Input.readObject();
        return ipList.get(keyMessage.getKey().hashCode()%ipList.size()-1);
    }
}
