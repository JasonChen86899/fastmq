package fastmq.client.consumer.serialization;

import java.io.IOException;

public interface ConsumerSerialization {

  Object decode(byte[] bytes) throws IOException;

  byte[] encode(Object o) throws IOException;

}
