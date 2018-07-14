package fastmq.broker.message;

import java.util.Map;

/**
 * Created by Jason Chen on 2016/10/2.
 */

/**
 * 消息的数据结构格式
 */
public class KeyMessage<K, V> implements Map.Entry<K, V>, Comparable<KeyMessage<K, V>> {

    private final String topic_name;
    private K key;
    private V value;
    private int patition;

    public KeyMessage(K k, V v, String tn) {
        key = k;
        value = v;
        topic_name = tn;
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

    public String getTopic_name() {
        return topic_name;
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

    public int getPatition() {
        return patition;
    }

    public void setPatition(int patition) {
        this.patition = patition;
    }

}
