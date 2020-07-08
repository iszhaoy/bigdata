package exec;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class JDBCUtils {
    public static void main(String[] args) {
        try {
            System.out.println(JDBCUtils.getConnection());
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
    private static DataSource ds;

    static {
        Properties pro = new Properties();
        try {
            pro.load(JDBCUtils.class.getClassLoader().getResourceAsStream("config.properties"));
            ds = DruidDataSourceFactory.createDataSource(pro);
        } catch (Exception e) {
            // TODO 自动生成的 catch 块
            e.printStackTrace();
        }
        System.out.println(ds);
    }

    public static Connection getConnection() throws SQLException {
        return ds.getConnection();
    }

    public static void close(ResultSet rs, Statement st, Connection con) {
        if (rs != null)
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        if (st != null)
            try {
                st.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        if (con != null)
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }

    }

    public static DataSource getDataSource() {
        return ds;
    }
}