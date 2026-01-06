package com.video.streaming.sink;

import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 自定义 Doris 字符串序列化器
 * 将 String 类型数据序列化为字节数组用于 Stream Load
 */
// 正确的接口实现
public class SimpleDorisStringSerializer implements DorisRecordSerializer<String> {

    @Override
    public DorisRecord serialize(String record) throws IOException {
        if (record == null || record.trim().isEmpty()) {
            return DorisRecord.empty;  // 返回空记录
        }

        byte[] bytes = record.getBytes(StandardCharsets.UTF_8);
        return DorisRecord.of(bytes);  // 创建 DorisRecord 对象
    }
}
