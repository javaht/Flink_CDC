import com.alibaba.fastjson.{JSON, JSONObject}
import com.ververica.cdc.connectors.mysql.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import com.ververica.cdc.debezium.DebeziumSourceFunction
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
import org.apache.flink.table.api.{$, AnyWithOperations, Table, lit}
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.util.Collector

import java.util.Date
import scala.concurrent.duration.Deadline
import scala.concurrent.duration.DurationConversions.fromNowConvert.R

object cdcDemo2 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    env.setParallelism(1)
    //2.Flink-CDC 将读取 binlog 的位置信息以状态的方式保存在 CK,如果想要做到断点续传,需要从 Checkpoint 或者 Savepoint 启动程序
    env.enableCheckpointing(5000L) //2.1 开启 Checkpoint,每隔 5 秒钟做一次 CK
    env.getCheckpointConfig.setCheckpointTimeout(1000) //检查点超时时间
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) //2.2 指定 CK 的一致性语义

     //2.3 设置任务关闭的时候保留最后一次 CK 数据
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L)) //2.4 指定从 CK 自动重启策略

      //2.5 设置状态后端
     env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flinkCDC"))
      //env.setStateBackend(new FsStateBackend("file:///D://tmp"))

      //2.6 设置访问 HDFS 的用户名
    System.setProperty("HADOOP_USER_NAME", "root")


    //使用Flink sql
    tableEnv.executeSql(
      """
        |CREATE TABLE user_info (
        |id int PRIMARY KEY NOT ENFORCED ,
        |name STRING,
        |sex STRING
        |) WITH (
        |      'connector' = 'mysql-cdc',
        |      'hostname' = '192.168.20.62',
        |      'port' = '3306',
        |      'username' = 'root',
        |      'password' = '123456',
        |      'server-time-zone' = 'Asia/Shanghai',
        |      'database-name' = 'cdc_test',
        |      'table-name' = 'user_info'
        |)
        |""".stripMargin)

    val date = new Date()
    val ts: Long = date.getTime
    val mysqlTable: Table = tableEnv.sqlQuery("select * from user_info")

    val tstable: Table = mysqlTable.addColumns(lit(ts))

     tableEnv.toChangelogStream(tstable).print()


        tableEnv.executeSql(
          """
            |CREATE TABLE hudi_user_info(
            |id      int PRIMARY KEY NOT ENFORCED,
            |name    String,
            |sex     String,
            |ts       BIGINT)
            |PARTITIONED BY (sex)
            |WITH (
            |  'connector' = 'hudi',
            |  'path' = 'hdfs://hadoop102:8020/zhouhengtao/hudi_user_info',
            |  'table.type' = 'COPY_ON_WRITE',
            |  'hoodie.datasource.write.keygenerator.class' = 'org.apache.hudi.keygen.ComplexAvroKeyGenerator',
            |  'hoodie.datasource.write.recordkey.field' = 'id',
            |  'hoodie.datasource.write.hive_style_partitioning' = 'true',
            |  'hive_sync.enable' = 'true',
            |  'hive_sync.mode' = 'hms',
            |   // 'scan.startup.mode' = 'latest-offset' ,
            |  'hive_sync.metastore.uris' = 'thrift://hadoop102:9083',
            |  'hive_sync.conf.dir'='C:\software\IdeaProjects\Flink_CDC\src\main\resources',
            |  'hive_sync.db' = 'hudi_ods',
            |  'hive_sync.table' = 'hudi_user_info',
            |  'hive_sync.partition_fields' = 'sex',
            |  'write.operation' ='upsert',
            |  'write.precombine.field' ='ts',
            |  'hive_sync.partition_extractor_class' = 'org.apache.hudi.hive.HiveStylePartitionValueExtractor')
            |""".stripMargin)


    tableEnv.executeSql("    insert into hudi_user_info select * from  "+tstable +" ")

    env.execute()
  }
}