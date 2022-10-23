package gew.filesystem.common;


import gew.filesystem.common.util.DefaultPropertyReader;

/**
 * Main Class (Not for Using)
 *
 * @author Jason/GeW
 * @since 2019-03-24
 */
public class Main {

    private static final DefaultPropertyReader PROPERTY = new DefaultPropertyReader("pom.properties");

    public static void main(String[] args) throws RuntimeException {
        System.out.printf("\nFile-System-Client Version: %s\n%n", PROPERTY.getVersion());
    }
}
