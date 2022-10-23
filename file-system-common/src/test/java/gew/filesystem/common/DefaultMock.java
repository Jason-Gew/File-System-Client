package gew.filesystem.common;

import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;

/**
 * @author Jason/GeW
 * @since 2022-10-22
 */
public class DefaultMock {

    @BeforeEach
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void setup() throws IllegalAccessException {
        Field[] fields = this.getClass().getDeclaredFields();
        for (Field field : fields) {
            Annotation[] annotations = field.getAnnotations();
            for (Annotation annotation : annotations) {
                if (annotation instanceof Mock) {
                    Class clazz = field.getType();
                    field.setAccessible(true);
                    field.set(this, Mockito.mock(clazz));
                }
            }
        }
    }
}
