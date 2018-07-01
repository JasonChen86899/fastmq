package fastmq.broker.consumer;

import com.github.zkclient.ZkClient;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Jason Chen on 2016/11/23.
 */
public class ConsumerGroup {

  private String name;

  private ArrayList<String> consumerIpList;

  private HashMap<String, List> collateMap;

  public ConsumerGroup(ZkClient zkClient, String path, String groupName) {
    this.name = groupName;
    this.consumerIpList = new ArrayList<>(zkClient.getChildren(path));
    this.collateMap = new HashMap<>();
  }

  public ArrayList<String> getConsumerIpList() {
    return consumerIpList;
  }

  public void setConsumerIpList(ArrayList<String> consumerIpList) {
    this.consumerIpList = consumerIpList;
  }


  public HashMap<String, List> getCollateMap() {
    return collateMap;
  }

  public void setCollateMap(HashMap<String, List> collateMap) {
    this.collateMap = collateMap;
  }

  public String getName() {
    return this.name;
  }
}

