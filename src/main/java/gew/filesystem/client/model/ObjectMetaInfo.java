package gew.filesystem.client.model;

import java.lang.reflect.Type;
import java.time.Instant;
import java.util.Map;
import java.util.HashMap;
import java.io.Serializable;


/**
 * @author Jason/GeW
 * @since 2019-03-24
 */
public class ObjectMetaInfo implements Serializable {

    private Map<String, String> userData = new HashMap<>();

    private Map<String, Object> metaData = new HashMap<>();

    private Map<String, Type> metaDataValueType = new HashMap<>();

    private ObjectType objectType;

    private Instant creationTime;

    private Long size;

    private static final long serialVersionUID = 20190324L;


    public ObjectMetaInfo() {

    }

    public Map<String, String> getUserData() {
        return userData;
    }

    public void setUserData(Map<String, String> userData) {
        this.userData = userData;
    }

    public Map<String, Object> getMetaData() {
        return metaData;
    }

    public void setMetaData(Map<String, Object> metaData) {
        this.metaData = metaData;
    }

    public Map<String, Type> getMetaDataValueType() {
        return metaDataValueType;
    }

    public void setMetaDataValueType(Map<String, Type> metaDataValueType) {
        this.metaDataValueType = metaDataValueType;
    }

    public Instant getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(Instant creationTime) {
        this.creationTime = creationTime;
    }

    public Long getSize() {
        return size;
    }

    public void setSize(Long size) {
        this.size = size;
    }

    public ObjectType getObjectType() {
        return objectType;
    }

    public void setObjectType(ObjectType objectType) {
        this.objectType = objectType;
    }

    @Override
    public String toString() {
        return "ObjectMetaInfo{" +
                "userData=" + userData +
                ", metaData=" + metaData +
                ", metaDataValueType=" + metaDataValueType +
                ", objectType=" + objectType +
                ", creationTime=" + creationTime +
                ", size=" + size +
                '}';
    }
}
