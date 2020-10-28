package com.wjy.flink.wc.sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import com.mysql.jdbc.Driver;
import com.wjy.flink.wc.vo.Wc;

/**
 * 数据存入mysql
 */
public class MysqlSink extends RichSinkFunction<Wc> {

    /**
     * 
     */
    private static final long serialVersionUID = -8773239282297800874L;

    private PreparedStatement ps;
    private Connection connection;

    // 创建数据库连接
    @SuppressWarnings("deprecation")
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        DriverManager.registerDriver(new Driver());
        // 从上下文获取配置参数，需要程序启动时放入env.getConfig().setGlobalJobParameters(paramFromProps);
        String dbUrl = String.format("jdbc:mysql://%s:%d/%s", parameters.getString("dbHost", "172.16.18.6"),
                parameters.getInteger("dbPort", 3306), parameters.getString("dbDataBase", "test"));
        connection = DriverManager.getConnection(dbUrl, parameters.getString("dbUser", "dba"),
                parameters.getString("dbPwd", "987654321"));
        ps = connection
                .prepareStatement(parameters.getString("dbInsertSql",
                        "insert into word_count_sum(word, frequency, create_time) values (?, ?, now()) "));
    }

    // 关闭数据库连接
    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    // 执行
    @Override
    public void invoke(Wc value, @SuppressWarnings("rawtypes") Context context) throws Exception {
        ps.setString(1, value.getWord());
        ps.setInt(2, value.getFrequency());
        ps.execute();
    }

}
