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

    /**
     * 这是创建group和topic的唯一接口，目前是这样的
     * @param map
     */
    public void creatGroupTop(Map<String,Integer> map){
        map.entrySet().stream().forEach((entry) -> {
            try {
                //每个topic 在Consumer/Topic目录下面,每个关注了该topic的groupid都会生成相应的子目录
                if(zkClient.exists("/Consumer/Topic/"+entry.getKey()+"/"+consumerConfig.getPropertiesMap().get("zk.groupid")))
                    return;//foreach 中不能用continue ；这里return 和 continue一样
                zkClient.createPersistent("/Consumer/Topic/"+entry.getKey()+"/"+consumerConfig.getPropertiesMap().get("zk.groupid"));
                //List<String> children = zkClient.getChildren("/Consumer/Topic/"+consumerConfig.getPropertiesMap().get("zk.groupid")+"/topic");
                //if(children.contains(entry.getKey()))
                    //return;//foreach 中不能用continue ；这里return 和 continue一样
                //zkClient.writeData("/Consumer/Group/"+consumerConfig.getPropertiesMap().get("zk.groupid")+"/topic",SerializationUtil.serialize(entry.getKey()));

                /**
                 * 调用注册topic的函数，这是关键的几步
                 */
                if(zkClient.exists("/Consumer/Topic/"+entry.getKey()))
                    return;
                PatitionCollate.registTopicEvent(entry.getKey(),entry.getValue());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
}
