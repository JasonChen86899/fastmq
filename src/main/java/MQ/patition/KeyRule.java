package MQ.patition;

import MQ.Message.KeyMessage;

/**
 * Created by Jason Chen on 2016/10/11.
 */

public interface KeyRule {
    default int setKeyRule(KeyMessage keyMessage){
        return keyMessage.getKey().hashCode();
    }
}
