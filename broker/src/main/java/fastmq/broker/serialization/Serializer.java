package fastmq.broker.serialization;

import java.io.IOException;

/**
 * @Description:
 * @Author: Jason Created in 2018/7/26
 */
public interface Serializer {

    byte[] serialize(Object object) throws IOException;

    Object deserialize(byte[] bytes) throws IOException;

    void closeIOManually() throws IOException;
}
