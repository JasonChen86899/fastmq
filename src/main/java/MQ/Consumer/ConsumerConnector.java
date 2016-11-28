package MQ.Consumer;

import MQ.Serialization.SerializationUtil;
import MQ.patition.PatitionCollate;
import com.github.zkclient.ZkClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Jason Chen on 2016/11/27.
 */
public class ConsumerConnector {
    private ConsumerConfig consumerConfig;
    private ZkClient zkClient;
    public ConsumerConnector(ConsumerConfig config){
        this.consumerConfig = config;
        String Zkservers = consumerConfig.getPropertiesMap().get("zk.connect");
        this.zkClient = new ZkClient(Zkservers,10000,10000);
    }

    public void creatGroupTopicMessageStreams(Map<String,Integer> map){
        map.entrySet().stream().forEach((entry) -> {
            try {
                //每个topic 在每个消费者组的topic目录下面
                List<String> children = zkClient.getChildren("/Consumer/Group/"+consumerConfig.getPropertiesMap().get("zk.groupid")+"/topic");
                if(children.contains(entry.getKey()))
                    return;//foreach 中不能用continue ；这里return 和 continue一样
                zkClient.writeData("/Consumer/Group/"+consumerConfig.getPropertiesMap().get("zk.groupid")+"/topic",SerializationUtil.serialize(entry.getKey()));
                PatitionCollate.registTopicEvent(entry.getKey(),entry.getValue());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
}
