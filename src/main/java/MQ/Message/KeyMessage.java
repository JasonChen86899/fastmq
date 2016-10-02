package MQ.Message;

import java.util.Map;

/**
 * Created by Jason Chen on 2016/10/2.
 */
public class KeyMessage<K,V> implements Map.Entry<K,V> {
    private final K key;
    private V value;
    public KeyMessage(K k,V v){
        key = k;
        value = v;
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
        return null;
    }
}
