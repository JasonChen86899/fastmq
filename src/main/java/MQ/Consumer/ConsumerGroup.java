package MQ.Consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Administrator on 2016/11/23.
 */
public class ConsumerGroup {

    private ArrayList<String> consumerIpList;

    private HashMap<String, List> collateMap;

    public ConsumerGroup() {
        this.consumerIpList = new ArrayList<>();
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
}

