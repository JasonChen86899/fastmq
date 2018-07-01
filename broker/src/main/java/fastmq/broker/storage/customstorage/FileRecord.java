package fastmq.broker.storage.customstorage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by JasonChen on 2016/12/8.
 */
public class FileRecord {

  private Lock fileLock;
  private File recordFile;
  private BufferedReader bufferedReader;
  private BufferedWriter bufferedWriter;

  public FileRecord(String filePath) throws IOException {
    recordFile = new File(filePath);
    bufferedReader = new BufferedReader(new FileReader(recordFile));
    bufferedWriter = new BufferedWriter(new FileWriter(recordFile));
    fileLock = new ReentrantLock();
  }

  /**
   * 一个topic_patition 一个主题保持一个文件记录，有利于文件操作 文件内容的格式是 top_patition+空格+计数
   */
  public void readqueueNumAndAdd(String topic_patition) throws IOException {
    fileLock.lock();
    String numString = bufferedReader.lines()
        .filter((eachLine) -> eachLine.split(" ")[0].equals(topic_patition)).findFirst().get()
        .split(" ")[1];
    BigInteger bigInteger = new BigInteger(numString);
    bigInteger.add(new BigInteger("1"));
    bufferedWriter.write(topic_patition + " " + bigInteger.toString());
    fileLock.unlock();
  }

}
