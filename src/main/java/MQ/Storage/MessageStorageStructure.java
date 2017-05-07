package MQ.Storage;

/**
 * Created by Jason Chen on 2016/10/11.
 */

import MQ.Message.KeyMessage;
import MQ.Storage.MessageNumberRecords.RecordsUtil;
import MQ.Storage.MessageNumberRecords.SqlDBUtil;
import org.rocksdb.RocksDBException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * 采用 key-value ，先对key进行加工用来保证消息的顺序性
 * 由于再三的思考，考虑到RocksDB的的特性（原理），key最好设定成String
 * 此处的加工方法我选择根据key的类型级别进行String化，然后再加上时间戳
 */
@Service
public class MessageStorageStructure {

    @Autowired
    private FastDB fastDB;

    @Resource(name = "redis")
    private RecordsUtil recordsUtil;

    //数据：key_序列号
    public boolean sycSaveMessage(KeyMessage<String,Object> keyMessage){
        String storekey;
        //key =  keyMessage.getKey();
        storekey = keyMessage.getTopic_name()+"_"+keyMessage.getPatition();
        try{
            String a= recordsUtil.selectMessageNumByKeyAndUpdateNum(storekey,"message_num");
            storekey += "_"+a;
            final String final_key = storekey;
            Callable<Boolean> save =() -> fastDB.putObject(final_key,keyMessage,keyMessage.getTopic_name(),true);
            FutureTask<Boolean> futureTask = new FutureTask<>(save);
            new Thread(futureTask).start();
            //同步操等待阻塞
            if(futureTask.get().booleanValue() == false)
                return false;
            //keyMessage.setKey(key);
            rePackageKeyMessage(final_key,keyMessage);
            return true;
        }catch (Exception e){
            return false;
        }
    }

    public boolean asycSaveMessage(KeyMessage<String,Object> keyMessage){
        String storekey,a;
        //key =  keyMessage.getKey();
        storekey = keyMessage.getTopic_name()+"_"+keyMessage.getPatition();
        try{
            a= recordsUtil.selectMessageNumByKeyAndUpdateNum(storekey,"message_num");
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
        storekey += "_"+a;
        final String final_key = storekey;
        Callable<Boolean> save =() -> fastDB.putObject(final_key,keyMessage,keyMessage.getTopic_name(),false);
        FutureTask<Boolean> futureTask = new FutureTask<>(save);
        new Thread(futureTask).start();
        //同步操等待阻塞
        try {
            if(futureTask.get().booleanValue() == false)
                return false;
        } catch (InterruptedException e) {
            return false;
        } catch (ExecutionException e) {
            return false;
        }
        //keyMessage.setKey(key);
        rePackageKeyMessage(final_key,keyMessage);
        return true;
    }

    /**
     * 当MQ有一台宕机之后，我们需要读取还没提交的记录继续放入队列
     * @param
     * @return
     */
    public void getMessageAndPutIntoQueue(String topic_patition, Queue<KeyMessage<String,Object>> messageQueue){
        String commited_num = recordsUtil.selectMessageCommited(topic_patition);
        String totalNum = recordsUtil.selectMessageTotalNum(topic_patition);
        BigInteger cn = new BigInteger(commited_num);
        BigInteger tn = new BigInteger(totalNum);
        while (cn.compareTo(tn)!=1){
            try {
                messageQueue.add((KeyMessage<String,Object>)fastDB.getObject(topic_patition.split("_")[0],topic_patition+(cn.toString())));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
            cn = cn.add(new BigInteger("1"));
        }
    }

    private KeyMessage<String,Object> rePackageKeyMessage(String newKey,KeyMessage<String,Object> oldKeyMessage){
        return new KeyMessage<>(newKey,oldKeyMessage,oldKeyMessage.getTopic_name());
    }

}
