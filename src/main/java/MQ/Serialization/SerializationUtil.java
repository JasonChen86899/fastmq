package MQ.Serialization;

import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Created by Jason Chen on 2016/10/17.
 */
public class SerializationUtil {
    public static byte[] serialize(Object object) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        Hessian2Output hessian2Output = new Hessian2Output();
        hessian2Output.init(byteArrayOutputStream);
        hessian2Output.writeObject(object);
        return byteArrayOutputStream.toByteArray();
    }

    public static Object deserialize(byte[] bytes) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        Hessian2Input hessian2Input = new Hessian2Input();
        hessian2Input.init(byteArrayInputStream);
        return hessian2Input.readObject();
    }
}
