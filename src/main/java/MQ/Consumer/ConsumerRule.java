package MQ.Consumer;

import MQ.Serialization.SerializationUtil;
import com.github.zkclient.ZkClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
     * 注入静态变量，需要定义getter，setter然后在 1.xml文件里面进行定义 2. 如下面的做法（stackoverflow上面分享的做法）
     * @param zkClient
     */
    @Autowired
    public void setZkClient(ZkClient zkClient) {
        ConsumerRule.zkClient = zkClient;
    }

    /**
     * 消费者组（Group）进行分组的规则也就是load-balance的策略
     * @param topic
     * @param group
     */
    public static void consumerRule(String topic, ConsumerGroup group){
        int patitionNum = 0;
        try {
            patitionNum = (int)SerializationUtil.deserialize(zkClient.readData("PatitionNum/"+topic));
        } catch (IOException e) {
            e.printStackTrace();
        }
        int consumerSize = group.getConsumerIpList().size();
        if(patitionNum<=consumerSize){
            group.setCollateMap(new HashMap<>());
            HashMap<String,List> collateMap = group.getCollateMap();
            for(int p=0; p<patitionNum; p++){
                List newList = new ArrayList<String>();
                newList.add(topic+"_"+(p+1));
                collateMap.put(group.getConsumerIpList().get(p), newList);
            }
        }else {
            int mul = patitionNum/consumerSize;
            int mod = patitionNum%consumerSize;
            group.setCollateMap(new HashMap<>());
            HashMap<String,List> collateMap = group.getCollateMap();
            for(int p=0,index =0; p<patitionNum; p=p+mul,index++){
                List newList = new ArrayList<String>();
                for(int q=0; q<mul; q++){
                    newList.add(topic+"_"+(p+1+q));
                }
                collateMap.put(group.getConsumerIpList().get(index),newList);
            }
            for(int q=mod-1; q>=0; q--){
                collateMap.get(group.getConsumerIpList().get(q)).add(topic+"_"+(patitionNum-q));
            }
        }
    }
}
