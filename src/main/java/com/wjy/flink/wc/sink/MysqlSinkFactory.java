package com.wjy.flink.wc.sink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import com.wjy.flink.wc.vo.Wc;

/**
 * 使用flink-connector-jdbc
 */
public class MysqlSinkFactory {

    public static SinkFunction<Wc> createSink(ParameterTool parameters) {
        String dbUrl = String.format(
                "jdbc:mysql://%s:%d/%s?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&useSSL=false",
                parameters.get("dbHost", "172.16.18.6"),
                parameters.getInt("dbPort", 3306), parameters.get("dbDataBase", "test"));
        String userName = parameters.get("dbUser", "dba");
        String pwd = parameters.get("dbPwd", "987654321");
        String sql = "insert into word_count_sum(word, frequency, create_time) values (?, ?, now()) ";

        return JdbcSink.sink(
                sql,
                (ps, record) -> {
                    ps.setString(1, record.getWord());
                    ps.setInt(2, record.getFrequency());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(10)
                        .withBatchIntervalMs(5000)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(dbUrl)
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername(userName)
                        .withPassword(pwd)
                        .build());
    }

}
