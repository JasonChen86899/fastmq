package MQ.Storage;

/**
 * Created by Jason Chen on 2016/10/11.
 */

import MQ.Message.KeyMessage;
import org.rocksdb.RocksDB;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.sql.Time;
import java.util.Date;
import java.util.Timer;

/**
 * 采用 key-value ，先对key进行加工用来保证消息的顺序性
 * 由于再三的思考，考虑到RocksDB的的特性（原理），key最好设定成String
 * 此处的加工方法我选择根据key的类型级别进行String化，然后再加上时间戳
 */
@Service
public class MessageStorageStructure {

    @Autowired
    private FastDB fastDB;

    public boolean saveMessage(String topic_name, KeyMessage<String,Object> keyMessage){
        String key;
        try{
            key =  keyMessage.getKey();
            key += "_"+String.valueOf(System.nanoTime());
            fastDB.putObject(key,keyMessage.getValue(),topic_name);
            return true;
        }catch (Exception e){
            return false;
        }
    }

    public KeyMessage<String,Object> getMessage(String topic_name, String key){

    }

}
