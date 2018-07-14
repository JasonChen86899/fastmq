package fastmq.broker.consumer;

import java.io.IOException;
import java.util.HashSet;

import MQ.Serialization.SerializationUtil;
import com.github.zkclient.ZkClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zeromq.ZMQ;

/**
 * Created by Jason Chen on 2016/11/23.
 */
@Service
public class ConsumerRegist extends Thread {

    @Autowired
    private ZkClient zkClient;
    private ZMQ.Context context;
    private ZMQ.Socket transfer;

    public ConsumerRegist(String tcpAddress) {
        this.context = ZMQ.context(1);
        this.transfer = context.socket(ZMQ.REP);
        this.transfer.bind(tcpAddress);
    }

    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            String group_consumerIp = transfer.recvStr();//block模式
            String[] recStrArray = group_consumerIp.split("_");
            String group = recStrArray[0];
            String consumerIp = recStrArray[1];
            if (check_ip(consumerIp)) {
                if (!zkClient.exists("/Consumer/Group/" + group + "/ids")) {
                    zkClient.createEphemeral("/Consumer/Group/" + group + "/ids");
                    HashSet<String> consumeripList = new HashSet<>();
                    consumeripList.add(consumerIp);
                    try {
                        zkClient.writeData("/Consumer/Group/" + group + "/ids",
                            SerializationUtil.serialize(consumeripList));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    try {
                        HashSet<String> consumeripList = (HashSet<String>) SerializationUtil
                            .deserialize(zkClient.readData("/Consumer/Group/" + group + "/ids"));
                        if (consumeripList.contains(consumerIp)) {
                            continue;
                        }
                        consumeripList.add(consumerIp);
                        zkClient.writeData("/Consumer/Group/" + group + "/ids",
                            SerializationUtil.serialize(consumeripList));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                //每次注册之后，需要进行一次重新分配

            } else {
                break;
            }
        }
    }

    private boolean check_ip(String ip) {
        if (ip.matches("tcp://[1-255]\\.[1-255]\\.[1-255]\\.[1-255]:[0-65535]")) {
            return true;
        } else {
            return false;
        }
    }
}
