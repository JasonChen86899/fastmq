package fastmq.broker.consumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.github.zkclient.ZkClient;
import fastmq.broker.serialization.SerializationUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by Jason Chen on 2016/11/23.
 */
@Service
public class ConsumerRule {

    /**
     * 消费者分配算法：
     */
    private static ZkClient zkClient;

    /**
     * 消费者组（Group）进行分组的规则也就是load-balance的策略
     */
    public static void DefalutConsumerRule(String topic, ConsumerGroup group) {
        HashMap<String, List> collateMap;
        int patitionNum = 0;
        try {
            patitionNum = (int) SerializationUtil.deserialize(zkClient.readData("PatitionNum/" + topic));
        } catch (IOException e) {
            e.printStackTrace();
        }
        int consumerSize = group.getConsumerIpList().size();
        if (patitionNum <= consumerSize) {
            group.setCollateMap(new HashMap<>());
            collateMap = group.getCollateMap();
            for (int p = 0; p < patitionNum; p++) {
                List newList = new ArrayList<String>();
                newList.add(topic + "_" + (p + 1));
                collateMap.put(group.getConsumerIpList().get(p), newList);
            }
        } else {
            int mul = patitionNum / consumerSize;
            int mod = patitionNum % consumerSize;
            group.setCollateMap(new HashMap<>());
            collateMap = group.getCollateMap();
            for (int p = 0, index = 0; p < patitionNum; p = p + mul, index++) {
                List newList = new ArrayList<String>();
                for (int q = 0; q < mul; q++) {
                    newList.add(topic + "_" + (p + 1 + q));
                }
                collateMap.put(group.getConsumerIpList().get(index), newList);
            }
            for (int q = mod - 1; q >= 0; q--) {
                collateMap.get(group.getConsumerIpList().get(q)).add(topic + "_" + (patitionNum - q));
            }
        }
        try {
            zkClient.writeData("/Consumer/Group/+" + group.getName() + "/collateMap",
                SerializationUtil.serialize(collateMap));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void OrderedConsumerRule(String topic, ConsumerGroup group) {
        if (zkClient.exists("Consumer/Group/" + group.getName() + "/collateMap")) {
            HashMap<String, List<String>> changeIp_topicpatitionList = new HashMap<>();
            try {
                HashMap<String, List<String>> old_consumerip_List_topicPatition = (HashMap<String, List<String>>) SerializationUtil
                    .deserialize(
                        zkClient.readData("Consumer/Group/" + group.getName() + "/collateMap"));
                if (group.getConsumerIpList().size() < old_consumerip_List_topicPatition.size()) {//下线
                    old_consumerip_List_topicPatition.forEach((key, value) -> {
                        if (!group.getConsumerIpList().contains(key)) {//找出下线的consumer ip
                            changeIp_topicpatitionList.put(key, value);
                        }
                    });
                    changeIp_topicpatitionList.forEach((key, value) -> {
                        old_consumerip_List_topicPatition.remove(key);
                        int new_consumer_size = group.getConsumerIpList().size();
                        if (value.size() <= new_consumer_size) {
                            for (int i = 0; i < value.size(); i++) {
                                old_consumerip_List_topicPatition
                                    .get(group.getConsumerIpList().get(i))
                                    .add(value.get(i));
                            }
                        } else {
                            int mul = value.size() / new_consumer_size;
                            int mod = value.size() % new_consumer_size;
                            for (int i = 0; i < new_consumer_size; i++) {
                                for (int j = 0; j < mul; j++) {
                                    old_consumerip_List_topicPatition
                                        .get(group.getConsumerIpList().get(i))
                                        .add(value.get(i + j * new_consumer_size));
                                }
                            }
                            for (int i = mod - 1; i >= 0; i--) {
                                old_consumerip_List_topicPatition
                                    .get(group.getConsumerIpList().get(i))
                                    .add(value.get(value.size() - i - 1));
                            }
                        }
                    });
                } else {//增加

                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            DefalutConsumerRule(topic, group);
        }


    }

    public static void ConsistentHashConsumerRule() {

    }

    private static void HashFunction() {

    }

    /**
     * 注入静态变量，需要定义getter，setter然后在 1.xml文件里面进行定义 2. 如下面的做法（stackoverflow上面分享的做法）
     */
    @Autowired
    public void setZkClient(ZkClient zkClient) {
        ConsumerRule.zkClient = zkClient;
    }
}
