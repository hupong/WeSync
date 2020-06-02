package com.luoboduner.wesync.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public abstract class DbUtil {
    private Connection connection = null;
    private Statement statement = null;
    private ResultSet resultSet = null;
    private static String DBUrl = null;
    private static String DBName = null;
    private static String DBUser = null;
    private static String DBPassword = null;

    private static DbUtil instance = null;

    private static final Logger logger = LoggerFactory.getLogger(DbUtil.class);

    /**
     * 私有的构造
     */
//    private DbUtil() {
//        loadConfig();
//    }

    /**
     * 获取实例，线程安全
     *
     * @return
     */
//    public static synchronized DbUtil getInstance() {
//        if (instance == null) {
//            instance = new DbUtil();
//        }
//        return instance;
//    }

    /**
     * 从配置文件加载设置数据库信息
     */
//    private abstract void loadConfig();

    /**
     * 获取连接，线程安全
     *
     * @return
     * @throws SQLException
     */
    public abstract /*synchronized*/ Connection getConnection() throws SQLException;

    /**
     * 测试连接，线程安全 参数从配置文件获取
     *
     * @return
     * @throws SQLException
     */
    public abstract /*synchronized*/ Connection testConnection() throws SQLException;

    /**
     * 测试连接，线程安全 参数从入参传入
     *
     * @return
     * @throws SQLException
     */
    public abstract /*synchronized*/ Connection testConnection(String dburl, String dbname, String dbuser, String dbpassword)
            throws SQLException;

    /**
     * 获取数据库声明，私有，线程安全
     *
     * @throws SQLException
     */
    private synchronized void getStatement() throws SQLException {
        getConnection();
        // 仅当statement失效时才重新创建
        if (statement == null || statement.isClosed()) {
            statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        }
    }

    /**
     * 关闭（结果集、声明、连接），线程安全
     *
     * @throws SQLException
     */
    public synchronized void close() throws SQLException {
        if (resultSet != null) {
            resultSet.close();
            resultSet = null;
        }
        if (statement != null) {
            statement.close();
            statement = null;
        }
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

    /**
     * 执行查询，线程安全
     *
     * @param sql
     * @return
     * @throws SQLException
     */
    public synchronized ResultSet executeQuery(String sql) throws SQLException {
        getStatement();
        if (resultSet != null && !resultSet.isClosed()) {
            resultSet.close();
        }
        resultSet = null;
        resultSet = statement.executeQuery(sql);
        return resultSet;
    }

    /**
     * 执行更新，线程安全
     *
     * @param sql
     * @return
     * @throws SQLException
     */
    public synchronized int executeUpdate(String sql) throws SQLException {
        int result = 0;
        getStatement();
        result = statement.executeUpdate(sql);
        return result;
    }

    /**
     * 执行任意，线程安全
     *
     * @param sql
     * @return
     * @throws SQLException
     */
    public synchronized boolean execute(String sql) throws SQLException {
        boolean result;
        getStatement();
        result = statement.execute(sql);
        return result;
    }

}
