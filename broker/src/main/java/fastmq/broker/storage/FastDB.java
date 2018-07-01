package fastmq.broker.storage;

import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by Jason Chen on 2016/9/29.
 */

/**
 * 关于RocksDB的一些说明： RocksDB 最好的支持是在Linux上，当然也能支持在Windows上，两者都需要配置JVM
 */
@Configuration
public class FastDB {

  /**
   * 静态变量 options 静态变量 db
   */
  private static Options options;
  private static RocksDB db;

  public FastDB() {
    openDatabase();
  }

  /**
   * 查看RocksDB 源码，里面没有List<ColumnFamilyHandle> 来记录列簇信息，这里用来记录这个信息
   *
   * private static final ConcurrentLinkedDeque<ColumnFamilyHandle> columnFamilyHandles = new
   * ConcurrentLinkedDeque<>();
   *
   *
   * /** 建立ConcurrentHashMap ，记录列簇名(key),ColumnFamilyHandle(value) 记录列簇名和列簇实例
   *
   * private static final ConcurrentHashMap<String,ColumnFamilyHandle> columnFamily_Name_Map = new
   * ConcurrentHashMap<>();
   **/
  @Bean
  public RocksDB openDatabase() {
    // a static method that loads the RocksDB C++ library.
    RocksDB.loadLibrary();
    // the Options class contains a set of configurable DB options
    // that determines the behavior of a database.
    options = new Options().setCreateIfMissing(true);
    db = null;
    try {
      db = RocksDB.open(options,
          "path/fastdb");// All of the contents of database are stored in this directory
    } catch (RocksDBException e) {
    }
    return db;
  }

  public void closeDatabase() {
      if (db != null) {
          db.close();
      }
    options.dispose();
  }

  //flag false 异步，true 同步
  public boolean putObject(Object key, Object value, String topic_name, boolean flag)
      throws IOException, RocksDBException {
    ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
    Hessian2Output hessian2Output = new Hessian2Output(byteArray);
    hessian2Output.writeObject(key);
    byte[] keyBytes = byteArray.toByteArray();
    hessian2Output.reset();
    hessian2Output.writeObject(value);
    byte[] valueBytes = byteArray.toByteArray();
    //db的open 静态方法有 关于初始化db时列簇和相应的列簇名的设置，不过也可以以后地动态添加
    ColumnFamilyHandle cf = db
        .createColumnFamily(new ColumnFamilyDescriptor(topic_name.getBytes()));
    try {
      db.put(cf, new WriteOptions().setSync(flag), keyBytes, valueBytes);
      return true;
    } catch (Exception e) {
        /*
        if(!columnFamily_Name_Map.containsKey(topic_name)) {
            columnFamilyHandles.add(cf);
            columnFamily_Name_Map.put(topic_name, cf);
        }
        */
      return true;
    }
  }

  public Object getObject(String columFamily_topic_name, Object key)
      throws IOException, RocksDBException {
    ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
    Hessian2Output hessian2Output = new Hessian2Output(byteArray);
    hessian2Output.writeObject(key);
    byte[] keyBytes = byteArray.toByteArray();
        /*
        if(columnFamily_Name_Map.containsKey(columFamily_topic_name)){
            byte[] bytes_get = db.get(columnFamily_Name_Map.get(columFamily_topic_name),keyBytes);
            ByteArrayInputStream byteArray_get = new ByteArrayInputStream(bytes_get);
            Hessian2Input hessian2Input = new Hessian2Input(byteArray_get);
            return hessian2Input.readObject();
        }else
            return null;
        */
    ColumnFamilyHandle cf = db
        .createColumnFamily(new ColumnFamilyDescriptor(columFamily_topic_name.getBytes()));
    byte[] bytes_get = db.get(cf, keyBytes);
      if (bytes_get != null) {
          ByteArrayInputStream byteArray_get = new ByteArrayInputStream(bytes_get);
          Hessian2Input hessian2Input = new Hessian2Input(byteArray_get);
          return hessian2Input.readObject();
      } else {
          return null;
      }
  }
}
