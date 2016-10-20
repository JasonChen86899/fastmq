package MQ.Storage;

/**
 * Created by Jason Chen on 2016/10/11.
 */

import MQ.Message.KeyMessage;
import org.rocksdb.RocksDBException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
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

    @Autowired
    private SqliteUtil sqliteUtil;

    public boolean sycSaveMessage(KeyMessage<String,Object> keyMessage){
        String key;
        try{
            key =  keyMessage.getKey();
            int a= sqliteUtil.selectMessageNumByKeyAndUpdateNum(key,keyMessage.getTopic_name());
            if(a<0)
                return false;
            key += "_"+a;
            final String final_key = key;
            Callable<Boolean> save =() -> fastDB.putObject(final_key,keyMessage.getValue(),keyMessage.getTopic_name(),true);
            FutureTask<Boolean> futureTask = new FutureTask(save);
            new Thread(futureTask).start();
            //同步操等待阻塞
            if(futureTask.get().booleanValue() == false)
                return false;
            keyMessage.setKey(key);
            return true;
        }catch (Exception e){
            return false;
        }
    }

    public boolean asycSaveMessage(KeyMessage<String,Object> keyMessage){
        String key;
        key =  keyMessage.getKey();
        int a= sqliteUtil.selectMessageNumByKeyAndUpdateNum(key,keyMessage.getTopic_name());
        if(a<0)
            return false;
        key += "_"+a;
        final String final_key = key;
        Callable<Boolean> save =() -> fastDB.putObject(final_key,keyMessage.getValue(),keyMessage.getTopic_name(),false);
        FutureTask<Boolean> futureTask = new FutureTask(save);
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
        keyMessage.setKey(key);
        return true;
    }

    /**
     * 当MQ有一台宕机之后，我们需要读取还没提交的记录继续放入队列
     * @param topic_name
     * @param key
     * @return
     */
    public void getMessageAndPutIntoQueue(String topic_name, String key, int sequenceId, Queue<KeyMessage<String,Object>> messageQueue){
        int commited_num = sqliteUtil.selectMessageCommited(key,topic_name);
        for(int i= commited_num+1;i<sequenceId;i++){
            try {
                messageQueue.add(new KeyMessage<String, Object>(key+"_"+i,fastDB.getObject(topic_name,key+"_"+i),topic_name));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        }
    }

}
