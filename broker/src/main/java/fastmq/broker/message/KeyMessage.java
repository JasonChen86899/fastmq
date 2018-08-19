package fastmq.broker.message;

import fastmq.common.serialization.SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by Jason Chen on 2016/10/2.
 */

/**
 * 消息的数据结构格式
 */
public class KeyMessage<K, V> implements Map.Entry<K, V>, Comparable<KeyMessage<K, V>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeyMessage.class);

    private final byte[] topicName;
    private K key;
    private V value;
    private int partition;

    public KeyMessage(K k, V v, byte[] tn) {
        key = k;
        value = v;
        topicName = tn;
    }

    @Override
    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public V setValue(V value) {
        this.value = value;
        return value;
    }

    public String getTopicName() {
        try {
            return (String)SerializationUtil.deserialize(topicName);
        } catch (Exception e) {
            LOGGER.error("KeyMessage topicName deserialize fail. key is {}",key);
            return null;
        }
    }

    @Override
    public int compareTo(KeyMessage<K, V> o) {
        String key1 = (String) this.key;
        String key2 = (String) o.getKey();
        if (Integer.valueOf(key1.split("_")[1]).compareTo(Integer.valueOf(key2.split("_")[1])) > 0) {
            return 1;
        } else if (Integer.valueOf(key1.split("_")[1]).compareTo(Integer.valueOf(key2.split("_")[1]))
            == 0) {
            return 0;
        } else {
            return -1;
        }
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

}
