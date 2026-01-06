package com.video.streaming.config;

import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

/**
 * 视频流处理配置类
 */
@Data
public class VideoStreamConfig implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(VideoStreamConfig.class);

    // Kafka配置
    private String kafkaBootstrapServers;
    private String kafkaTopic;
    private String kafkaGroupId;

    // Doris配置
    private String dorisFenodes;
    private String dorisDatabase;
    private String dorisTable;
    private String dorisUsername;
    private String dorisPassword;

    // OSS配置
    private String ossEndpoint;
    private String ossAccessKeyId;
    private String ossAccessKeySecret;
    private String ossBucket;

    // YOLO配置
    private String yoloModelPath;
    private float yoloConfidenceThreshold;

    // 视频处理配置
    private long videoSegmentDuration;
    private long keyframeMinInterval;

    /**
     * 从配置文件加载配置
     */
    public static VideoStreamConfig loadConfig() {
        VideoStreamConfig config = new VideoStreamConfig();

        try (InputStream input = VideoStreamConfig.class
                .getClassLoader()
                .getResourceAsStream("application.properties")) {

            if (input == null) {
                LOG.warn("Unable to find application.properties, using default values");
                return getDefaultConfig();
            }

            Properties prop = new Properties();
            prop.load(input);

            // Kafka配置
            config.setKafkaBootstrapServers(
                    prop.getProperty("kafka.bootstrap.servers", "localhost:9092"));
            config.setKafkaTopic(
                    prop.getProperty("kafka.topic", "video-stream-topic"));
            config.setKafkaGroupId(
                    prop.getProperty("kafka.group.id", "video-processing-group"));

            // Doris配置
            config.setDorisFenodes(
                    prop.getProperty("doris.fenodes", "localhost:8030"));
            config.setDorisDatabase(
                    prop.getProperty("doris.database", "video_analytics"));
            config.setDorisTable(
                    prop.getProperty("doris.table", "video_detections"));
            config.setDorisUsername(
                    prop.getProperty("doris.username", "root"));
            config.setDorisPassword(
                    prop.getProperty("doris.password", ""));

            // OSS配置
            config.setOssEndpoint(
                    prop.getProperty("oss.endpoint", "http://oss-cn-hangzhou.aliyuncs.com"));
            config.setOssAccessKeyId(
                    prop.getProperty("oss.access.key.id", ""));
            config.setOssAccessKeySecret(
                    prop.getProperty("oss.access.key.secret", ""));
            config.setOssBucket(
                    prop.getProperty("oss.bucket", "video-storage"));

            // YOLO配置
            config.setYoloModelPath(
                    prop.getProperty("yolo.model.path", "D:\\workspace\\video-stream-processing\\src\\main\\resources\\models\\yolov8n.onnx"));
            config.setYoloConfidenceThreshold(
                    Float.parseFloat(prop.getProperty("yolo.confidence.threshold", "0.5")));

            // 视频处理配置
            config.setVideoSegmentDuration(
                    Long.parseLong(prop.getProperty("video.segment.duration", "180000"))); // 3分钟
            config.setKeyframeMinInterval(
                    Long.parseLong(prop.getProperty("keyframe.min.interval", "5000"))); // 5秒

            LOG.info("Configuration loaded successfully");

        } catch (Exception e) {
            LOG.error("Error loading configuration", e);
            return getDefaultConfig();
        }

        return config;
    }

    /**
     * 获取默认配置
     */
    private static VideoStreamConfig getDefaultConfig() {
        VideoStreamConfig config = new VideoStreamConfig();

        // Kafka默认配置
        config.setKafkaBootstrapServers("localhost:9092");
        config.setKafkaTopic("video-stream-topic");
        config.setKafkaGroupId("video-processing-group");

        // Doris默认配置
        config.setDorisFenodes("localhost:8030");
        config.setDorisDatabase("video_analytics");
        config.setDorisTable("video_detections");
        config.setDorisUsername("root");
        config.setDorisPassword("");

        // OSS默认配置
        config.setOssEndpoint("http://oss-cn-hangzhou.aliyuncs.com");
        config.setOssAccessKeyId("");
        config.setOssAccessKeySecret("");
        config.setOssBucket("video-storage");

        // YOLO默认配置
        config.setYoloModelPath("D:\\workspace\\video-stream-processing\\src\\main\\resources\\models\\yolov8n.onnx");
        config.setYoloConfidenceThreshold(0.5f);

        // 视频处理默认配置
        config.setVideoSegmentDuration(180000); // 3分钟
        config.setKeyframeMinInterval(5000); // 5秒

        return config;
    }
}