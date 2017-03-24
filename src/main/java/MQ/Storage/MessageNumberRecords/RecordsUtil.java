package MQ.Storage.MessageNumberRecords;

/**
 * Created by Jason Chen on 2017/3/24.
 */
public interface RecordsUtil {
    String selectMessageNumByKeyAndUpdateNum(String topic_partition,String H) throws Exception ;
    String selectMessageTotalNum(String topic_patition);
    String selectMessageCommited(String topic_patition);
}
