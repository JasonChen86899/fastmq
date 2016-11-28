package MQ.Consumer;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Jaosn Chen on 2016/11/27.
 */
public class ConsumerConfig {
    /**
     * Map 参数 这两个参数是必须的
     * 1.zk.connect
     * 2.zk.groupid
     */

     private Map<String,String> propertiesMap;
    {
        propertiesMap = new HashMap<>();
        propertiesMap.put("zk.connect",null);
        propertiesMap.put("zk.groupid",null);
    }
     public ConsumerConfig(Map<String,String> pMap){
        if(pMap.get("zk.connect")!=null){
            propertiesMap.put("zk.connect",pMap.get("zk.connect"));
        }
        if(pMap.get("zk.groupid")!=null){
            propertiesMap.put("zk.groupid",pMap.get("zk.groupid"));
        }
     }

    public Map<String, String> getPropertiesMap() {
        return this.propertiesMap;
    }
}
