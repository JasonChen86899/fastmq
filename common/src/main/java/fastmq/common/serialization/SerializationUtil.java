package fastmq.common.serialization;

import fastmq.common.serialization.hessian.HessianSerializer;
import fastmq.common.serialization.hessian.PooledHessianSerializerFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * Created by Jason Chen on 2016/10/17.
 */
public class SerializationUtil {

    private static final ObjectPool<HessianSerializer> HESSIAN_SERIALIZER_OBJECT_POOL =
        new GenericObjectPool<>(new PooledHessianSerializerFactory(), new GenericObjectPoolConfig<>());

    public static byte[] serialize(Object object) throws Exception {
        HessianSerializer pooledObject = HESSIAN_SERIALIZER_OBJECT_POOL.borrowObject();
        byte[] result = pooledObject.serialize(object);
        HESSIAN_SERIALIZER_OBJECT_POOL.returnObject(pooledObject);

        return result;
    }

    public static Object deserialize(byte[] bytes) throws Exception {
        HessianSerializer pooledObject = HESSIAN_SERIALIZER_OBJECT_POOL.borrowObject();
        Object result = pooledObject.deserialize(bytes);
        HESSIAN_SERIALIZER_OBJECT_POOL.returnObject(pooledObject);

        return result;
    }


}
