
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import java.text.SimpleDateFormat
import java.util.Date

object cdcDemo {
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
     // env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flinkCDC"))
      env.setStateBackend(new FsStateBackend("file:///D://tmp"))

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
        |      'table-name' = 'user_info',
        |      'jdbc.properties.useSSL' = 'false',
        |      'scan.startup.mode' = 'earliest-offset',
        |      'scan.snapshot.fetch.size' ='1024',
        |      'debezium.mysql.include.schema.changes'='true',
        |      'debezium.snapshot.locking.mode' = 'none'
        |)
        |""".stripMargin)


        val date = new Date()
        val ts: Long = date.getTime
        val partitionpath_format: SimpleDateFormat = new SimpleDateFormat("yyyy")
        val years = partitionpath_format.format(date)

        val usertable: Table = tableEnv.sqlQuery("select * from user_info");
        val tstable: Table = usertable.addColumns(lit(ts),lit(years))
        tableEnv.toChangelogStream(tstable).print()

        tableEnv.executeSql(
          """
            |
            |    CREATE TABLE hudi_user_infos(
            |     id      int PRIMARY KEY NOT ENFORCED,
            |     name    String,
            |     sex     String,
            |     ts       BIGINT,
            |     years     String)
            |    PARTITIONED BY (years)
            |    WITH (
            |     'connector' = 'hudi',
            |     'path' = 'hdfs://hadoop102:8020/zhouhengtao/hudi_user_infos',
            |     'table.type' = 'COPY_ON_WRITE',
            |     'hooddatasource.write.recordkey.field' = 'id,ts',
            |     'hoodie.index.type'='BLOOM',
            |     'hoodie.datasource.write.hive_style_partitioning' = 'true',
            |     'hoodie.datasource.write.partitionpath.urlencode' = 'true',
            |     'write.operation' ='upsert',
            |     'write.precombine.field' ='ts',
            |     'hive_sync.enabled' = 'true',
            |     'hive_sync.mode' = 'hms',
            |     'hive_sync.metastore.uris' = 'thrift://hadoop102:9083',
            |     'hive_sync.db' = 'hudi_ods',
            |     'hive_sync.table' = 'hudi_user_infos',
            |     'hive_sync.assume_date_partitioning' = 'true',  //假设分区格式是yyyy/mm/dd 假定数据表是按日期分区的。这意味着，如果参数设置为True，则Hive Sync将在将数据写入到 Hive 表时自动创建日期分区。如果该参数设置为 False，则 Hive Sync 将不会自动创建分区，需要手动创建分区。
            |     'hive_sync.partition_fields' = 'years', //HiveSync使用的分区字段。如果设置了该参数，则 Hive Sync 将按照指定的字段对数据进行分区，而不是默认的日期分区。在设置该参数时，可以指定多个字段，以逗号分隔。
            |     'hive_sync.support_timestamp'= 'true', //是否支持时间戳类型 为true则 Hive Sync 将支持时间戳类型，并在将数据写入到 Hive 表时自动将时间戳字段转换为 Timestamp 类型
            |     'write.keygenerator.class' = 'org.apache.hudi.keygen.ComplexAvroKeyGenerator', //是 Apache Hudi 提供的一个具体的键生成器类，它可以生成复杂的 Avro 格式的键。该键生成器可以在数据写入 Hudi 数据存储时用于对数据记录进行标识，以便对其进行管理和查询。
            |     'scan.startup.mode' = 'latest-offset' , //首次使用initial
            |     'hive_sync.partition_extractor_class' = 'org.apache.hudi.hive.HiveStylePartitionValueExtractor' //是 Apache Hudi 提供的一个具体的分区提取器类，它可以提取数据中的分区信息，并按照 Hive 风格生成分区目录。通过使用该分区提取器，可以在将数据写入 Hive 表时，更好地管理数据的存储和查询。
            |""".stripMargin)


    tableEnv.executeSql("    insert into hudi_user_infos select * from  "+tstable +" ")

    env.execute()
  }
}