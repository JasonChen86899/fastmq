package MQ;

import com.caucho.hessian.io.Hessian2Output;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Jason Chen on 2016/9/29.
 */

/** 关于RocksDB的一些说明：
 * RocksDB 最好的支持是在Linux上，这个JNI就是一个基于Linux系统编译的RocksJNI jar包（Maven 中央仓库支持的也是这个），Windows则需要重新引入基于Windows编译的Jar包
 */
public class RocksDBUtil {
    /**
     * 静态变量 options 实例变量
     * 静态变量 db 实例变量
     */
    private static Options options;
    private static RocksDB db;
    /**
     * 查看RocksDB 源码，里面没有List<ColumnFamilyHandle> 来记录列簇信息，这里用来记录这个信息
     */
    private static ArrayList<ColumnFamilyHandle> columnFamilyHandles;

    public static void openDatabase(){
        // a static method that loads the RocksDB C++ library.
        RocksDB.loadLibrary();
        // the Options class contains a set of configurable DB options
        // that determines the behavior of a database.
        options = new Options().setCreateIfMissing(true);
        db = null;
        try{
            db = RocksDB.open(options,"path/to/db");// All of the contents of database are stored in this directory
        }catch (RocksDBException e){
        }
    }

    public static void closeDatabase(){
        if(db != null)
            db.close();
        options.dispose();
    }

    public static void putObject(Object key,Object value) throws IOException, RocksDBException {
        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        Hessian2Output hessian2Output = new Hessian2Output(byteArray);
        hessian2Output.writeObject(key);
        byte[] keyBytes = byteArray.toByteArray();
        hessian2Output.reset();
        hessian2Output.writeObject(value);
        byte[] valueBytes = byteArray.toByteArray();
        //db的open 静态方法有 关于初始化db时列簇和相应的列簇名的设置，不过也以后地动态添加
        ColumnFamilyHandle cf;
        db.put(cf=db.createColumnFamily("topic_name"),keyBytes,valueBytes);
        columnFamilyHandles.add(cf);
    }

    public static void getObject(String columFamily_topic_name, Object key) throws IOException {
        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        Hessian2Output hessian2Output = new Hessian2Output(byteArray);
        hessian2Output.writeObject(key);
        byte[] keyBytes = byteArray.toByteArray();
        columnFamilyHandles.stream().forEach(columnFamilyHandle -> {});
    }
}
