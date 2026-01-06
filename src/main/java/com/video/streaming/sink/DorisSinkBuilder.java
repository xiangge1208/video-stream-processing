package com.video.streaming.sink;

import com.video.streaming.config.VideoStreamConfig;
import com.video.streaming.model.Detection;
import com.video.streaming.model.DetectionResult;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * Doris Sink构建器
 */
public class DorisSinkBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(DorisSinkBuilder.class);

    public static void addToDoris(DataStream<String> stream, VideoStreamConfig config) {
        // 检查是否配置了Doris参数
        if (config.getDorisFenodes() == null || config.getDorisFenodes().isEmpty() ||
            config.getDorisDatabase() == null || config.getDorisDatabase().isEmpty() ||
            config.getDorisTable() == null || config.getDorisTable().isEmpty()) {
            
            LOG.warn("Doris configuration is incomplete. Skipping Doris sink. " +
                    "Fenodes: {}, Database: {}, Table: {}", 
                    config.getDorisFenodes(), config.getDorisDatabase(), config.getDorisTable());
            return; // 跳过Doris sink，而不是抛出异常
        }

        try {
            // 确保配置参数不为null
            String fenodes = config.getDorisFenodes();
            String database = config.getDorisDatabase();
            String table = config.getDorisTable();
            String username = config.getDorisUsername();
            String password = config.getDorisPassword();

            // Doris配置
            DorisOptions dorisOptions = DorisOptions.builder()
                    .setFenodes(fenodes)
                    .setTableIdentifier(database + "." + table)
                    .setUsername(username != null ? username : "")
                    .setPassword(password != null ? password : "")
                    .build();

            // 执行选项
            Properties streamLoadProp = new Properties();
            streamLoadProp.setProperty("format", "json");
            streamLoadProp.setProperty("strip_outer_array", "false");
            
            DorisExecutionOptions executionOptions = DorisExecutionOptions.builder()
                    .setLabelPrefix("flink_doris")
                    .setBufferSize(128 * 1024) // 128KB
                    .setBufferCount(2)
                    .setBufferFlushIntervalMs(5000)
                    .setStreamLoadProp(streamLoadProp)
                    .enable2PC()
                    .build();

            // 创建Sink并添加到流
            DorisSink<String> sink = DorisSink.<String>builder()
                    .setDorisReadOptions(DorisReadOptions.builder().build())
                    .setDorisExecutionOptions(executionOptions)
                    .setDorisOptions(dorisOptions)
                    .build();

            stream.sinkTo(sink).name("DorisSink-" + database + "." + table)
                    .uid("doris-sink-" + table.replace("_", "-"));

            LOG.info("Doris sink configured: {}.{}, bufferSize=128KB, bufferCount=2, flushInterval=5000ms", 
                    database, table);
        } catch (Exception e) {
            LOG.error("Failed to configure Doris sink: {}", e.getMessage(), e);
            LOG.info("Continuing without Doris sink...");
            // 继续执行，不抛出异常
        }
    }

    /**
     * 转换DetectionResult为JSON字符串
     */
    public static String convertToJson(DetectionResult result) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        StringBuilder json = new StringBuilder();

        if (result.getDetections() == null || result.getDetections().isEmpty()) {
            return null;
        }

        for (Detection detection : result.getDetections()) {
            json.append("{")
                    .append("\"stream_id\":\"").append(result.getStreamId()).append("\",")
                    .append("\"detection_time\":\"").append(sdf.format(new Date(result.getTimestamp()))).append("\",")
                    .append("\"frame_id\":").append(result.getFrameId()).append(",")
                    .append("\"object_class\":\"").append(detection.getObjectClass()).append("\",")
                    .append("\"confidence\":").append(detection.getConfidence()).append(",")
                    .append("\"bbox_x1\":").append(detection.getBbox().getX1()).append(",")
                    .append("\"bbox_y1\":").append(detection.getBbox().getY1()).append(",")
                    .append("\"bbox_x2\":").append(detection.getBbox().getX2()).append(",")
                    .append("\"bbox_y2\":").append(detection.getBbox().getY2()).append(",")
                    .append("\"frame_url\":\"").append(result.getFrameUrl() != null ? result.getFrameUrl() : "").append("\"")
                    .append("}\n");
        }

        return json.toString();
    }
}