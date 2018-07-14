package MQ;

import java.io.IOException;

import MQ.Message.KeyMessage;
import MQ.Serialization.SerializationUtil;
import MQ.Storage.MessageStorageStructure;
import MQ.patition.PatitionCollate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zeromq.ZMQ;

/**
 * Created by Jason Chen on 2016/10/21.
 */
@Service
public class ReceiveFromLeader extends Thread {

    @Autowired
    private MessageStorageStructure messageStorage;
    @Autowired
    private MessageQueueMap messageQueueMap;
    private boolean flag;
    private boolean synStorage;
    private String tcp;
    private int type;
    private ZMQ.Context context;
    private ZMQ.Socket transfer;

    public ReceiveFromLeader(String ipAddress, int t, boolean syn) {
        this.flag = true;
        this.synStorage = true;//默认是true
        this.tcp = ipAddress;
        this.type = t;
        this.context = ZMQ.context(1);
        this.transfer = context.socket(type);
        transfer.bind(tcp);
    }

    public void run() {
        while (flag) {
            //1KB的信息量,用来基本的信息传输，这个值是暂时的设定
            byte[] revice_bytes = new byte[1024];
            transfer.recv(revice_bytes, 0, 1024, 1);
            KeyMessage<String, Object> msg;
            try {
                msg = (KeyMessage<String, Object>) SerializationUtil.deserialize(revice_bytes);
            } catch (IOException e) {
                msg = null;
            }
            if (msg != null) {
                if (synStorage == true) {
                    //开始将信息进行存储,同步刷盘
                    if (messageStorage.sycSaveMessage(msg)) {
                        try {
                            if (tcp == PatitionCollate.getIpAddressByTopicPatition(msg.getTopic_name(), msg)) {
                                new PutMessageToQueue(msg, messageQueueMap).start();
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
                if (synStorage == false) {
                    //异步刷盘,这里的异步刷盘只是DB的异步，真正的异步需要别的方案，要好好想想
                    if (messageStorage.asycSaveMessage(msg)) {
                        try {
                            if (tcp == PatitionCollate.getIpAddressByTopicPatition(msg.getTopic_name(), msg)) {
                                new PutMessageToQueue(msg, messageQueueMap).start();
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    public void setFlag(boolean stop) {
        this.flag = stop;
    }
}
