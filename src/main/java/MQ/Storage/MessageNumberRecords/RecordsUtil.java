package MQ.Storage.MessageNumberRecords;

/**
 * Created by Jason Chen on 2017/3/24.
 */
public interface RecordsUtil {

    //自增计数，可以是消息总数，也可以是已经消费的消费总数
    String selectMessageNumByKeyAndUpdateNum(String topic_partition, String H) throws Exception;

    //查询消息总数
    String selectMessageTotalNum(String topic_patition);

    //查询已消费消息总数
    String selectMessageCommited(String topic_patition);
}
