package util;

import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.Properties;

public class SysProperties {
    private static Logger log = Logger.getLogger(SysProperties.class);
    private static SysProperties sysProperties = null;
    private static Properties properties = null;
    private static final String PROPERTY_FILE = "application.properties";

    public static SysProperties getInstance(){
        if(sysProperties == null){
            sysProperties = new SysProperties();
        }
        return sysProperties;
    }

    private SysProperties(){
        ClassLoader loader = SysProperties.class.getClassLoader();
        if( loader == null){
            loader = ClassLoader.getSystemClassLoader();
        }
        InputStream inputStream = loader.getResourceAsStream(PROPERTY_FILE);

        properties = new Properties();

        try {
            properties.load(inputStream);
        } catch (Exception ex) {
            log.error("Unable to load properties");
        }
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

}
