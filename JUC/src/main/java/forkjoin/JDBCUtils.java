package forkjoin;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;


public class JDBCUtils {
	public static void main(String[] args) {
		System.out.println(JDBCUtils.getConn());
	}
	private static Properties prop = new Properties();

	private JDBCUtils() {
	}

	static {
		try {
			ClassLoader classLoader = JDBCUtils.class.getClassLoader();
			String path = classLoader.getResource("config.properties")
					.getPath();
			prop.load(new FileInputStream(path));

		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}

	}
	
	public static Connection getConn() {
		Connection conn;
		try {
			String driverClass = prop.getProperty("driverClass");
			String jdbcUrl = prop.getProperty("jdbcUrl");
			String user = prop.getProperty("user");
			String password = prop.getProperty("password");

			Class.forName(driverClass);
			conn = DriverManager.getConnection(jdbcUrl, user, password);
			return conn;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	public static void close(Connection conn, Statement stat, ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				rs = null;
			}
		}
		if (stat != null) {
			try {
				stat.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				stat = null;
			}
		}
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				conn = null;
			}
		}

	}

}
