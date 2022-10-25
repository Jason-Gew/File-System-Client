package gew.filesystem.common.model;

import java.io.Serializable;

/**
 * Short Property Info
 *
 * @author Jason/GeW
 * @since  2019-03-24
 */
public class ObjectProperty implements Serializable {

    private String name;

    private Boolean isDirectory;

    private Long size;

    private static final long serialVersionUID = 20190324L;


    public ObjectProperty() {

    }

    public ObjectProperty(String name) {
        this.name = name;
    }

    public ObjectProperty(String name, Boolean isDirectory) {
        this.name = name;
        this.isDirectory = isDirectory;
    }

    public ObjectProperty(String name, Boolean isDirectory, Long size) {
        this.name = name;
        this.isDirectory = isDirectory;
        this.size = size;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Boolean getDirectory() {
        return isDirectory;
    }

    public void setDirectory(Boolean directory) {
        isDirectory = directory;
    }

    @Override
    public String toString() {
        return "ObjectProperty{" +
                "name='" + name + '\'' +
                ", isDirectory=" + isDirectory +
                ", size=" + size +
                '}';
    }
}
