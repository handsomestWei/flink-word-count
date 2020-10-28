package com.wjy.flink.wc.table;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import com.wjy.flink.wc.ds.ArrayDataSource;

/**
 * 使用table api统计分词
 */
public class WordCount {

    @SuppressWarnings("serial")
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 使用StreamTable
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 数据源为自定义的字符串数组
        DataStreamSource<String> ds0 = env.fromElements(ArrayDataSource.WORDS);

        // 转换为二元组，元素是：String型单词，Integer型单词个数
        DataStream<Tuple2<String, Integer>> ds = ds0.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                // 按空格拆分单词
                for (String word : value.split("\\s")) {
                    // 每个单词个数记为1
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        // 定义表结构，scala表达式
        Table wcTable = tEnv.fromDataStream(ds, Expressions.$("word"), Expressions.$("frequency"));
        wcTable.printSchema();

        // 创建视图
        tEnv.createTemporaryView("word_count", wcTable);

        // 执行sql并输出
        // tEnv.sqlQuery("select word, sum(frequency) as frequency from word_count group by word ").execute().print();
        tEnv.sqlQuery("select word, frequency from word_count ").execute().print();
    }

}
