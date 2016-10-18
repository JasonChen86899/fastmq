package MQ.Storage;

import org.springframework.stereotype.Service;

import java.sql.*;

/**
 * Created by Jason Chen on 2016/10/15.
 */

/**
 * 每台机器需要这一个小型数据库进行key message_num commited 等信息的记录,key表示的是消息的ID，不含序列号
 */
@Service
public class SqliteUtil {
    /**
     * 数据库连接
     */
    private Connection sqLite_connection;

    public SqliteUtil(){
        //这边设置根据具体的系统（Windows或者Linux或者MacOS）
        String Native_URL = "jdbc:sqlite:C:/fast_sqlite.db";
        try{
            Class.forName("org.sqlite.JDBC");
            sqLite_connection = DriverManager.getConnection(Native_URL);
        }catch(ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public int selectMessageNumByKeyAndUpdateNum(String key,String topic_name){
        //如果数据量很大，则需要每天生成一个数据表 Records_日期
        int a =0;
        String select_sql = "SELECT"+"message_num"+"FROM "+"Records"+"WHERE"+"key"+" = "+key+
                "AND"+"topic_name"+" = "+topic_name;
        String update_sql = "UPDATE"+"Records"+"SET"+"message_num"+" = "+(a+1)+"WHERE"+"key "+" = "+key+
                "AND"+"topic_name"+" = "+topic_name;
        PreparedStatement preparedStatement =null;
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
        return a;
    }

    public int selectMessageCommited(String key,String topic_name){
        int a =0;
        String select_sql = "SELECT"+"commited_num"+"FROM "+"Records"+"WHERE"+"key"+" = "+key+
                "AND"+"topic_name"+" = "+topic_name;
        PreparedStatement preparedStatement =null;
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
        return a;
    }

    public boolean updateCommitedNum(String key,String topic_name){
        String update_sql = "UPDATE"+"Records"+"SET"+"commited_num"+" = "+"commited_num+1"+"WHERE"+"key "+" = "+key+
                "AND"+"topic_name"+" = "+topic_name;
        PreparedStatement preparedStatement =null;
        try {
            preparedStatement = sqLite_connection.prepareStatement(update_sql);
            preparedStatement.execute();
        } catch (SQLException e) {
            return false;
            //e.printStackTrace();
        }
        return true;
    }

}
