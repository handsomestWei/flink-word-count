package com.wjy.flink.wc.ds;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import com.mysql.jdbc.Driver;

/**
 * 从mysql数据源读取数据
 */
public class MysqlDataSource extends RichSourceFunction<List<String>> {

    /**
     * 
     */
    private static final long serialVersionUID = -5390353174342202336L;

    private Connection connection = null;
    private PreparedStatement ps = null;
    private volatile boolean isRunning = true;

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
        ps = connection.prepareStatement(parameters.getString("dbQuerySql", "select text from word_count "));
    }

    // 执行查询并获取结果
    @Override
    public void run(SourceContext<List<String>> ctx)
            throws Exception {
        List<String> textQueryList = new ArrayList<String>();
        try {
            if (isRunning) {
                ResultSet resultSet = ps.executeQuery();
                while (resultSet.next()) {
                    String text = resultSet.getString("text");
                    textQueryList.add(text);
                }
                // 查询结果放入上下文，并添加时间戳和水位线
                ctx.collectWithTimestamp(textQueryList, System.currentTimeMillis());
                ctx.emitWatermark(new Watermark(System.currentTimeMillis()));
                textQueryList.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 关闭数据库连接
    @Override
    public void cancel() {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        isRunning = false;
    }

}
