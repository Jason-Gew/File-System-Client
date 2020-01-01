package gew.filesystem.client.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author Jason/GeW
 * @since 2019-03-24
 */
public class MetaDataPair implements Serializable {

    private String key;

    private String value;

    public MetaDataPair() {

    }

    public MetaDataPair(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetaDataPair that = (MetaDataPair) o;
        return Objects.equals(key, that.key) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @Override
    public String toString() {
        return "MetaDataPair{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
