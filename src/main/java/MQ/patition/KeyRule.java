package MQ.patition;

import MQ.Message.KeyMessage;

/**
 * Created by Jason Chen on 2016/10/11.
 */

public interface KeyRule {

    //默认的key的规则，由于key根据持久化的要求变成key_序列号的结构，所以需要默认的key的规则的制定
    //需要采集 _ 前面的key
    default int setKeyRule(KeyMessage<String, Object> keyMessage) {
        return keyMessage.getKey().split("_")[0].hashCode();
    }
}
