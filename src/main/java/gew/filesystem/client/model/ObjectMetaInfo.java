package gew.filesystem.client.model;

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


    private static final long serialVersionUID = 20190324L;

    
}
