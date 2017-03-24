package MQ.Storage.MessageNumberRecords;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigInteger;

/**
 * Created by Jason Chen on 2016/10/15.
 */

/**
 * 需要一个小型数据库进行key message_num commited 等信息的记录,key表示的是消息的ID，不含序列号
 */
@Service(value = "sqlDB")
public class SqlDBUtil implements RecordsUtil{
    /**
     * 数据库连接采用spring jdbc 不需要采用orm框架
     */
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Transactional(isolation = Isolation.SERIALIZABLE)//序列化操作，不考虑REPEATABLE_READ,因为会出现很多次的事务回滚
    public String selectMessageNumByKeyAndUpdateNum(String topic_patition,String zd) throws Exception {
        //如果数据量很大，则需要每天生成一个数据表 Records_日期
        String a ="";
        String select_sql = "SELECT"+zd+"FROM "+"Records"+"WHERE"+
                "topic_patition_name"+" = "+topic_patition;

        /*PreparedStatement preparedStatement =null;
        ResultSet resultSet = null;
        try {
            preparedStatement = sqLite_connection.prepareStatement(select_sql);
            preparedStatement.execute();
            resultSet = preparedStatement.getResultSet();
            a = resultSet.getInt(1);
        } catch (SQLException e) {
            return -1;
            //e.printStackTrace();
        }
        try {
            preparedStatement = sqLite_connection.prepareStatement(update_sql);
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            return -2;
        }
        return a;*/

        String b = jdbcTemplate.queryForObject(select_sql,String.class);
        a = new BigInteger(b).add(new BigInteger("1")).toString();
        String update_sql = "UPDATE"+"Records"+"SET"+zd+" = "+a+"WHERE"+
                "topic_patition_name"+" = "+topic_patition;
        if(jdbcTemplate.update(update_sql)!=1)
            throw new Exception("update error");
        return a;
    }

    public String selectMessageTotalNum(String topic_patition){
        String select_sql = "SELECT"+"message_num"+"FROM "+"Records"+"WHERE"+
                "topic_patition_name"+" = "+topic_patition;
        return jdbcTemplate.queryForObject(select_sql,String.class);
    }

    public String selectMessageCommited(String topic_patition){
        String select_sql = "SELECT"+"commited_num"+"FROM "+"Records"+"WHERE"+
                "topic_patition_name"+" = "+topic_patition;
        /*PreparedStatement preparedStatement =null;
        ResultSet resultSet = null;
        try {
            preparedStatement = sqLite_connection.prepareStatement(select_sql);
            preparedStatement.execute();
            resultSet = preparedStatement.getResultSet();
            a = resultSet.getInt(1);
        } catch (SQLException e) {
            return -1;
            //e.printStackTrace();
        }
        return a;*/
        String a = jdbcTemplate.queryForObject(select_sql,String.class);
        return a;
    }
}
