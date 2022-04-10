package util;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 数据库信息
 */
@Data
@AllArgsConstructor
public class DataBaseInfo {
    private String driver;
    private String url;
    private String user;
    private String password;
}
