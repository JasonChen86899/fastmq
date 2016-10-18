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
public class KeyMessage<K,V> implements Map.Entry<K,V> {
    private K key;
    private V value;
    private final String topic_name;

    public KeyMessage(K k,V v,String tn){
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

}
