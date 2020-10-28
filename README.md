# flink-word-count
基于flink1.11.2的单词统计，包括table api和streaming使用，以及常见的数据源读写

# 分层API
Flink 根据抽象程度分层，提供了三种不同的 API。每一种 API 在简洁性和表达力上有着不同的侧重，并且针对不同的应用场景。
<div align=center><img width="506" height="178" src="https://github.com/handsomestWei/flink-word-count/blob/main/design/flink-api.png" /></div>

# Usage

## 使用table api
[Code Link](https://github.com/handsomestWei/flink-word-count/blob/main/src/main/java/com/wjy/flink/wc/table/WordCount.java)
### 定义表结构
```
Table wcTable = tEnv.fromDataStream(ds, Expressions.$("word"), Expressions.$("frequency"));
```
### 创建视图
```
tEnv.createTemporaryView("word_count", wcTable);
```
### 执行flink sql
```
tEnv.sqlQuery("select word, frequency from word_count ").execute().print();
```

## 使用streaming
[Code Link](https://github.com/handsomestWei/flink-word-count/blob/main/src/main/java/com/wjy/flink/wc/streaming/WordCount.java)
### 输入数据源为socket流
```
DataStreamSource<String> textDs = env.socketTextStream(hostName, port, "\n");
```
### 自定义水位线
```
ds.assignTimestampsAndWatermarks(WatermarkStrategy
    // 延迟三秒
    .forBoundedOutOfOrderness(Duration.ofSeconds(3)));
```
### 自定义输出
```
ds.addSink(new StdPrintSink());

// 定义
@PublicEvolving
public class StdPrintSink<IN> extends PrintSinkFunction<IN> {

    @Override
    public void invoke(IN record) {
        super.invoke((IN) ("word count print>" + record.toString()));
    }

}
```

## 读写mysql
[Code Link](https://github.com/handsomestWei/flink-word-count/blob/main/src/main/java/com/wjy/flink/wc/streaming/WordCountV2.java)
### 读取外部配置文件参数
```
// 获取外部参数 --configPath /xxx/xx/config.properties
ParameterTool parameters = ParameterTool.fromArgs(args);

// 读取configPath参数的值：配置文件目录
String configPath = parameters.get("configPath", null);

// 读取配置文件
ParameterTool paramFromProps = ParameterTool.fromPropertiesFile(configPath);

// 把配置参数传入运行时上下文
env.getConfig().setGlobalJobParameters(paramFromProps);
```
### 设定时间特征
```
// 设定事件时间
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
```
### 自定义mysql数据源
<details>
<summary>继承RichSourceFunction，从mysql读取数据</summary>
<pre><code>
public class MysqlDataSource extends RichSourceFunction<List<String>> {
 
    @Override
    public void open(Configuration parameters) throws Exception {
        // 创建数据库连接
        // 从上下文获取配置参数
		String dbHost = parameters.getString("dbHost");
        ...
    }

    @Override
    public void run(SourceContext<List<String>> ctx)
            throws Exception {
        // 执行查询并获取结果
		...
        // 查询结果放入上下文，并添加时间戳和水位线
        ctx.collectWithTimestamp(textQueryList, System.currentTimeMillis());
        ctx.emitWatermark(new Watermark(System.currentTimeMillis()));
        textQueryList.clear();
    }
   
    @Override
    public void cancel() {
		// 关闭数据库连接
		...
    }

}
</code></pre>
</details>

### 读取mysql数据
```
DataStream<List<String>> ds0 = env.addSource(new MysqlDataSource());
```

### 自定义mysql sink
<details>
<summary>继承RichSinkFunction，数据存入mysql</summary>
<pre><code>
public class MysqlSink extends RichSinkFunction<Wc> {

    @Override
    public void open(Configuration parameters) throws Exception {
        // 创建数据库连接
		...
    }

    @Override
    public void close() throws Exception {
		// 关闭数据库连接
        ...
    }

    @Override
    public void invoke(Wc value, @SuppressWarnings("rawtypes") Context context) throws Exception {
		// 执行
        ps.setString(1, value.getWord());
        ps.setInt(2, value.getFrequency());
        ps.execute();
    }
}
</code></pre>
</details>

### 写入mysql
```
ds.addSink(new MysqlSink());
// 提交执行
env.execute("Mysql WordCount");
```
