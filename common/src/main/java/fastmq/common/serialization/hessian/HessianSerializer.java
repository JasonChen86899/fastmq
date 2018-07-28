package fastmq.common.serialization.hessian;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;
import fastmq.common.serialization.Serializer;
import org.apache.commons.io.IOUtils;

/**
 * @Description:
 * @Author: Jason Created in 2018/7/26
 */
public class HessianSerializer implements Serializer {

    private ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    private Hessian2Output hessian2Output = new Hessian2Output();

    private Hessian2Input hessian2Input = new Hessian2Input();

    public byte[] serialize(Object object) throws IOException {
        hessian2Output.init(byteArrayOutputStream);
        hessian2Output.writeObject(object);
        hessian2Output.flush();
        byte[] result = byteArrayOutputStream.toByteArray();

        byteArrayOutputStream.reset();

        return result;
    }

    public Object deserialize(byte[] bytes) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);

        hessian2Input.init(byteArrayInputStream);
        Object object = hessian2Input.readObject();

        byteArrayInputStream.close();
        byteArrayInputStream = null;

        return object;
    }

    public void closeIOManually() throws IOException {
        hessian2Output.close();
        hessian2Input.close();
        // 使用IOUtils
        IOUtils.closeQuietly(byteArrayOutputStream);
    }
}
