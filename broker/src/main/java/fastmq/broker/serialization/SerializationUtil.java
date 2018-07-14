package fastmq.broker.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;

/**
 * Created by Jason Chen on 2016/10/17.
 */
public class SerializationUtil {

    public static byte[] serialize(Object object) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        Hessian2Output hessian2Output = new Hessian2Output();

        try {
            hessian2Output.init(byteArrayOutputStream);
            hessian2Output.writeObject(object);
        } finally {
            // TODO 使用IOUtils
            hessian2Output.close();
            byteArrayOutputStream.close();
        }

        return byteArrayOutputStream.toByteArray();
    }

    public static Object deserialize(byte[] bytes) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        Hessian2Input hessian2Input = new Hessian2Input();

        try {
            hessian2Input.init(byteArrayInputStream);
        } finally {
            // TODO 使用IOUtils
            hessian2Input.close();
            byteArrayInputStream.close();
        }

        return hessian2Input.readObject();
    }
}
