package gew.filesystem.common.model;


import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

/**
 * MetaData Pair
 * @author Jason/GeW
 * @since  2019-03-24
 */
@Getter
@Setter
@EqualsAndHashCode
public class MetaDataPair implements Serializable {

    private String key;

    private String value;

    public MetaDataPair() {

    }

    public MetaDataPair(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("key", key)
                .append("value", value)
                .toString();
    }
}
