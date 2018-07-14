package MQ.Storage.MessageNumberRecords;

import javax.annotation.Resource;

import org.springframework.data.redis.core.HashOperations;
import org.springframework.stereotype.Service;

/**
 * Created by Jason Chen on 2017/3/24.
 */

/**
 * 此类是操作redis的技术类，基于两个redis hash表 records:message_num ,records:commit_num 分别记录以topic_partition为key的vaule值：消息计数，已消费消息计数，用以查询和自增。
 */
@Service(value = "redis")
public class RedisUtil implements RecordsUtil {

    @Resource(name = "defaultHashOperations")
    private HashOperations<String, String, Long> hashOps;

    public String selectMessageNumByKeyAndUpdateNum(String topic_partition, String H) throws Exception {
        return String.valueOf(hashOps.increment(H, topic_partition, 1));
    }

    public String selectMessageTotalNum(String topic_patition) {
        return String.valueOf(hashOps.get("records:message_num", topic_patition));
    }

    public String selectMessageCommited(String topic_patition) {
        return String.valueOf(hashOps.get("records:commit_num", topic_patition));
    }
}
