
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.data.Struct;

public class MyDeserialization implements DebeziumDeserializationSchema<String>{
        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
            //获取主题信息,包含着数据库和表名
            String topic = sourceRecord.topic();
            String[] arr = topic.split("\\.");
            String db = arr[1];
            String tableName = arr[2];

            //获取操作类型 是READ DELETE UPDATE CREATE
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);
            //获取值信息并转换为Struct类型
            Struct value = (Struct) sourceRecord.value();
            //获取变化后的数据 只有新增和更改数据有after
            Struct after = value.getStruct("after");
            JSONObject data = new JSONObject();
            if(after != null){
                //创建JSON对象用于存储数据信息
                for (Field field : after.schema().fields()) {
                    Object o = after.get(field);
                    data.put(field.name(), o);
                }
            }else{
                // 删除操作没有after，但是有before
                Struct before = value.getStruct("before");
                for (Field field : before.schema().fields()) {
                    Object o = before.get(field);
                    data.put(field.name(), o);
                }
            }

            //创建JSON对象用于封装最终返回值数据信息
            JSONObject result = new JSONObject();
            result.put("operation", operation.toString().toLowerCase());
            result.put("data", data);
            result.put("database", db);
            result.put("table", tableName);

            //发送数据至下游
            collector.collect(result.toJSONString());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }


}