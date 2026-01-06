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
            return;
        }

        try {
            String fenodes = config.getDorisFenodes();
            String database = config.getDorisDatabase();
            String table = config.getDorisTable();
            String username = config.getDorisUsername() != null ? config.getDorisUsername() : "";
            String password = config.getDorisPassword() != null ? config.getDorisPassword() : "";

            LOG.info("Configuring Doris sink: fenodes={}, db={}, table={}, user={}",
                    fenodes, database, table, username);

            // Doris连接配置
            DorisOptions.Builder dorisBuilder = DorisOptions.builder()
                    .setFenodes(fenodes)
                    .setTableIdentifier(database + "." + table);

            if (!username.isEmpty()) {
                dorisBuilder.setUsername(username);
            }
            if (!password.isEmpty()) {
                dorisBuilder.setPassword(password);
            }

            DorisOptions dorisOptions = dorisBuilder.build();

            // Stream Load配置
            Properties streamLoadProp = new Properties();
            streamLoadProp.setProperty("format", "json");
            streamLoadProp.setProperty("strip_outer_array", "false");
            streamLoadProp.setProperty("read_json_by_line", "true");

            // 执行选项
            DorisExecutionOptions executionOptions = DorisExecutionOptions.builder()
                    .setLabelPrefix("flink_video_" + System.currentTimeMillis())
                    .setBufferSize(128 * 1024) // 128KB
                    .setBufferCount(2)
                    .setBufferFlushIntervalMs(5000)
                    .setMaxRetries(3)
                    .setStreamLoadProp(streamLoadProp)
                    .build();

            // 创建Sink - 使用自定义序列化器
            DorisSink<String> sink = DorisSink.<String>builder()
                    .setDorisReadOptions(DorisReadOptions.builder().build())
                    .setDorisExecutionOptions(executionOptions)
                    .setDorisOptions(dorisOptions)
                    .setSerializer(new SimpleDorisStringSerializer())
                    .build();

            stream.sinkTo(sink)
                    .name("DorisSink-" + database + "." + table)
                    .uid("doris-sink-" + table.replace("_", "-"));

            LOG.info("Doris sink configured successfully: {}.{}", database, table);

        } catch (Exception e) {
            LOG.error("Failed to configure Doris sink: {}", e.getMessage(), e);
            LOG.info("Continuing without Doris sink...");
        }
    }

    /**
     * 转换DetectionResult为JSON字符串（每行一个检测对象）
     */
    public static String convertToJson(DetectionResult result) {
        if (result.getDetections() == null || result.getDetections().isEmpty()) {
            return null;
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        StringBuilder json = new StringBuilder();

        for (Detection detection : result.getDetections()) {
            json.append("{")
                    .append("\"stream_id\":\"").append(escape(result.getStreamId())).append("\",")
                    .append("\"detection_time\":\"").append(sdf.format(new Date(result.getTimestamp()))).append("\",")
                    .append("\"frame_id\":").append(result.getFrameId()).append(",")
                    .append("\"object_class\":\"").append(escape(detection.getObjectClass())).append("\",")
                    .append("\"confidence\":").append(detection.getConfidence()).append(",")
                    .append("\"bbox_x1\":").append(detection.getBbox().getX1()).append(",")
                    .append("\"bbox_y1\":").append(detection.getBbox().getY1()).append(",")
                    .append("\"bbox_x2\":").append(detection.getBbox().getX2()).append(",")
                    .append("\"bbox_y2\":").append(detection.getBbox().getY2()).append(",")
                    .append("\"frame_url\":\"").append(result.getFrameUrl() != null ? escape(result.getFrameUrl()) : "").append("\"")
                    .append("}\n");
        }

        return json.toString();
    }

    /**
     * 转义JSON特殊字符
     */
    private static String escape(String str) {
        if (str == null) return "";
        return str.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }
}
