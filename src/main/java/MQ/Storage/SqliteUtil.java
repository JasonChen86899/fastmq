package MQ.Storage;

import org.springframework.stereotype.Service;

import java.sql.*;

/**
 * Created by Jason Chen on 2016/10/15.
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

    public int selectMessageNumByKeyAndUpdateNum(String key){
        //如果数据量很大，则需要每天生成一个数据表 Records_日期
        int a =0;
        String select_sql = "SELECT"+"message_num"+"FROM "+"Records"+"WHERE"+"key"+" = "+key;
        String update_sql = "UPDATE"+"Records"+"SET"+"message_num"+" = "+(a+1)+"WHERE"+"key "+" = "+key;
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
}
