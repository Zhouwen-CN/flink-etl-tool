package com.etl.core.utils;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;

/**
 * 序列化工具类
 */
public final class SerializerUtils {
    public static byte[] serialize(Object obj) {
        if (obj == null) {
            return null;
        }
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(512);
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(obj);
            return bos.toByteArray();
        } catch (final IOException ex) {
            throw new UnsupportedOperationException("can not serialize object", ex);
        }
    }

    public static Object deserialize(byte[] serialized) throws IOException {
        if (serialized == null) {
            return null;
        }
        try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
             ObjectInputStream ois =
                     new ObjectInputStream(bis) {
                         @Override
                         protected Class<?> resolveClass(ObjectStreamClass osc)
                                 throws IOException, ClassNotFoundException {
                             // make sure use current thread classloader
                             ClassLoader cl = Thread.currentThread().getContextClassLoader();
                             if (cl == null) {
                                 return super.resolveClass(osc);
                             }
                             return Class.forName(osc.getName(), false, cl);
                         }
                     }) {
            return ois.readObject();
        } catch (final ClassNotFoundException | IOException ex) {
            throw new UnsupportedOperationException("can not deserialize object", ex);
        }
    }
}

