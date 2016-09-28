package MQ;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

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
}
