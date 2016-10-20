package MQ.Message;

import java.util.Map;

/**
 * Created by Jason Chen on 2016/10/2.
 */

/**
 * 消息的数据结构格式
 * @param <K>
 * @param <V>
 */
public class KeyMessage<K,V> implements Map.Entry<K,V> ,Comparable<KeyMessage<K,V>>{
    private K key;
    private V value;
    private final String topic_name;

    public KeyMessage(K k, V v, String tn){
        key = k;
        value = v;
        topic_name = tn;
    }

    @Override
    public K getKey() {
        return key;
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

    public void setKey(K key) {
        this.key = key;
    }

    public String getTopic_name() {
        return topic_name;
    }

    @Override
    public int compareTo(KeyMessage<K, V> o) {
        String key1 = (String)this.key;
        String key2 = (String)o.getKey();
        if(Integer.valueOf(key1.split("_")[1]).compareTo(Integer.valueOf(key2.split("_")[1]))>0)
            return 1;
        else if(Integer.valueOf(key1.split("_")[1]).compareTo(Integer.valueOf(key2.split("_")[1]))==0)
            return 0;
        else return -1;
    }
}
