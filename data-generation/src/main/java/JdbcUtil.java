import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * 获取到db.properties文件中的数据库信息
 */
public class JdbcUtil {
    /**
     * 通过读取Properties
     *
     * @return 获取数据库信息
     */
    public static DataBaseInfo getDataBaseInfo() throws IOException {
        Properties properties = new Properties();
        InputStream resourceAsStream = JdbcUtil.class.getClassLoader().getResourceAsStream("db.properties");
        properties.load(resourceAsStream);
        String driver = properties.getProperty("driver");
        String url = properties.getProperty("url");
        String user = properties.getProperty("user");
        String password = properties.getProperty("password");
        return new DataBaseInfo(driver, url, user, password);
    }

    public static Connection getConnection() throws IOException, ClassNotFoundException, SQLException {
        DataBaseInfo dataBaseInfo = getDataBaseInfo();
        Class.forName(dataBaseInfo.getDriver());
        return DriverManager.getConnection(dataBaseInfo.getUrl(), dataBaseInfo.getUser(), dataBaseInfo.getPassword());
    }
}
