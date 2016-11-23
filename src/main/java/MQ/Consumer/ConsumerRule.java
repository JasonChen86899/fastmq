package MQ.Consumer;

import MQ.Serialization.SerializationUtil;
import com.github.zkclient.ZkClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

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

    public static void consumerRule(String topic, ConsumerGroup group){
        try {
            int patitionNum = (int)SerializationUtil.deserialize(zkClient.readData("PatitionNum/"+topic));
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
