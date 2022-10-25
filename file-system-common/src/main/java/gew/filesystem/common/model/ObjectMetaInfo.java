package gew.filesystem.common.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.lang.reflect.Type;
import java.time.Instant;
import java.util.Map;
import java.util.HashMap;
import java.io.Serializable;


/**
 * Object Meta Data & Information
 *
 * @author Jason/GeW
 * @since  2019-03-24
 */
@Getter
@Setter
@ToString
public class ObjectMetaInfo implements Serializable {

    private Map<String, String> userData = new HashMap<>();

    private Map<String, Object> metaData = new HashMap<>();

    private Map<String, Type> metaDataValueType = new HashMap<>();

    private ObjectType objectType;

    private String contentType;

    private Instant creationTime;

    private Instant lastModified;

    private Long size;

    private static final long serialVersionUID = 20190324L;


    public ObjectMetaInfo() {

    }

    public ObjectMetaInfo(Map<String, String> userData) {
        if (userData != null) {
            this.userData = userData;
        }
    }

}
