package fastmq.client.consumer;

import MQ.patition.PatitionCollate;
import com.github.zkclient.ZkClient;
import fastmq.client.consumer.serialization.ConsumerSerialization;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Created by Jason Chen on 2016/11/27.
 */
public class ConsumerConnector {

  private ConsumerConfig consumerConfig;
  private ZkClient zkClient;
  private ConsumerSerialization consumerSerialization;

  public ConsumerConnector(ConsumerConfig config,ConsumerSerialization cs) {
    this.consumerConfig = config;
    String Zkservers = consumerConfig.getPropertiesMap().get("zk.connect");
    this.zkClient = new ZkClient(Zkservers, 10000, 10000);
    this.consumerSerialization = cs;
  }

  /**
   * 这是创建group和topic的唯一接口，目前是这样的
   */
  public void creatGroupTopic(Map<String, Integer> map) {
    for (Entry<String, Integer> entry : map.entrySet()) {
      try {
        /**
         * 调用注册topic的函数，这是关键的几步
         */
        if (zkClient.exists("/Consumer/Topic/" + entry.getKey())) {
          //每个topic 在Consumer/Topic目录下面,每个关注了该topic的groupid都会生成相应的子目录
          if (zkClient.exists(
              "/Consumer/Topic/" + entry.getKey() + "/" + consumerConfig.getPropertiesMap()
                  .get("zk.groupid"))) {
            continue;
          }
          zkClient.createPersistent(
              "/Consumer/Topic/" + entry.getKey() + "/" + consumerConfig.getPropertiesMap()
                  .get("zk.groupid"));
        } else {
          zkClient.createPersistent(
              "/Consumer/Topic/" + entry.getKey() + "/" + consumerConfig.getPropertiesMap()
                  .get("zk.groupid"));
          PatitionCollate.registTopicEvent(zkClient, entry.getKey(), entry.getValue());
        }
        //跟新消费者组 组内成员信息
        String group = consumerConfig.getPropertiesMap().get("zk.groupid");
        String consumerIp = consumerConfig.getPropertiesMap().get("consumer.ip");

        if (!zkClient.exists("/Consumer/Group/" + group + "/ids")) {
          zkClient.createEphemeral("/Consumer/Group/" + group + "/ids");
          HashSet<String> consumeripList = new HashSet<>();
          consumeripList.add(consumerIp);
          try {
            zkClient.writeData("/Consumer/Group/" + group + "/ids",
                consumerSerialization.encode(consumeripList));
          } catch (IOException e) {
            e.printStackTrace();
          }
        } else {
          try {
            HashSet<String> consumeripList = (HashSet<String>) consumerSerialization
                .decode(zkClient.readData("/Consumer/Group/" + group + "/ids"));
            if (consumeripList.contains(consumerIp)) {
              continue;
            }
            consumeripList.add(consumerIp);
            zkClient.writeData("/Consumer/Group/" + group + "/ids",
                consumerSerialization.encode(consumeripList));
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
