package fastmq.common.serialization.hessian;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * @Description:
 * @Author: Jason Created in 2018/7/27
 */
public class PooledHessianSerializerFactory implements PooledObjectFactory<HessianSerializer> {

    @Override
    public PooledObject<HessianSerializer> makeObject() throws Exception {
        return new DefaultPooledObject<>(new HessianSerializer());
    }

    @Override
    public void destroyObject(PooledObject<HessianSerializer> pooledObject) throws Exception {
        pooledObject.getObject().closeIOManually();
    }

    @Override
    public boolean validateObject(PooledObject<HessianSerializer> pooledObject) {
        return false;
    }

    @Override
    public void activateObject(PooledObject<HessianSerializer> pooledObject) throws Exception {

    }

    @Override
    public void passivateObject(PooledObject<HessianSerializer> pooledObject) throws Exception {

    }
}
