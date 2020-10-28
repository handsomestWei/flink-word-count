package com.wjy.flink.wc.streaming;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import com.wjy.flink.wc.sink.StdPrintSink;

/**
 * 使用streaming统计分词
 */
public class WordCount {

    // unix环境启动netcat 执行 nc -lk 9000
    // windows环境安装 https://nmap.org/ncat/ 执行ncat -lk 9000
    @SuppressWarnings({ "serial", "rawtypes", "unchecked" })
    public static void main(String[] args) throws Exception {

        ParameterTool paramFromProps = null;
        String hostName = "localhost";
        int port = 9000;

        // 获取外部参数 --configPath /xxx/xx/config.properties
        ParameterTool parameters = ParameterTool.fromArgs(args);

        // 读取参数：配置文件目录configPath
        String configPath = parameters.get("configPath", null);
        if (configPath != null) {
            // 读取配置文件
            paramFromProps = ParameterTool.fromPropertiesFile(configPath);
            hostName = paramFromProps.get("hostName", hostName);
            port = paramFromProps.getInt("port", port);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 把配置参数存入上下文
        if (paramFromProps != null) {
            env.getConfig().setGlobalJobParameters(paramFromProps);
        }

        // 数据源为socket流。监听9000端口，每行输入作为一条记录，转换为DataStream
        DataStreamSource<String> textDs = env.socketTextStream(hostName, port, "\n");
        
        // 过滤
        DataStream<String> ds0 = textDs.filter(new FilterFunction<String>() {
            
            @Override
            public boolean filter(String value) throws Exception {
                // 去掉内容为simple的记录
                return !"simple".equals(value);
            }
        });
        
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
        
        // 自定义水位线：延迟3秒
        ds.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)));

        // 自定义key选择器
        ds.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {

            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                // 返回元组的第0列单词
                return value.getField(0);
            }

        })
                .timeWindow(Time.seconds(3))// 定义时间窗口
                .sum(1)// 统计第1列单词个数的总数
                .addSink(new StdPrintSink());// 结果输出到控制台

        // 提交
        env.execute("Simple Streaming WordCount");
    }

}
