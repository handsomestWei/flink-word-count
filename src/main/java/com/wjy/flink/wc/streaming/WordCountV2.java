package com.wjy.flink.wc.streaming;

import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import com.wjy.flink.wc.ds.MysqlDataSource;
import com.wjy.flink.wc.sink.MysqlSink;
import com.wjy.flink.wc.vo.Wc;

/**
 * 单词统计：从mysql获取数据，并写入mysql
 */
public class WordCountV2 {

    @SuppressWarnings("serial")
    public static void main(String[] args) throws Exception {

        // 获取外部参数 --configPath /xxx/xx/config.properties
        ParameterTool parameters = ParameterTool.fromArgs(args);

        // 读取参数：配置文件目录configPath
        String configPath = parameters.get("configPath", null);

        // 读取配置文件
        ParameterTool paramFromProps = ParameterTool.fromPropertiesFile(configPath);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 把配置参数存入上下文
        env.getConfig().setGlobalJobParameters(paramFromProps);
        
        // 设定时间特征为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 从mysql获取数据
        DataStream<List<String>> ds0 = env.addSource(new MysqlDataSource());

        // 转换为二元组，元素是：String型单词，Integer型单词个数
        DataStream<Tuple2<String, Integer>> ds = ds0
                // .broadcast()// 广播
                .flatMap(new FlatMapFunction<List<String>, Tuple2<String, Integer>>() {

                    @Override
                    public void flatMap(List<String> value, Collector<Tuple2<String, Integer>> out)
                            throws Exception {
                        for (String text : value) {
                            for (String word : text.split("\\s")) {
                                // 每个单词个数记为1
                                out.collect(Tuple2.of(word, 1));
                            }
                        }
                    }
        });

        // 统计单词数
        ds.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {

            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                // 返回元组的第0列单词
                return value.getField(0);
            }

        }).timeWindow(Time.seconds(2))// 将时间窗口内的数据聚合
                .sum(1)
                .map(new MapFunction<Tuple2<String, Integer>, Wc>() { // Tuple2<String, Integer>元组转换为对象Wc

                    @Override
                    public Wc map(Tuple2<String, Integer> value) throws Exception {
                        Wc wc = new Wc();
                        wc.setWord(value.getField(0));
                        wc.setFrequency(value.getField(1));
                        return wc;
                    }

                })
                // .addSink(MysqlSinkFactory.createSink(paramFromProps))
                .addSink(new MysqlSink()); // 数据写入mysql

        env.execute("Mysql WordCount");

    }

}
