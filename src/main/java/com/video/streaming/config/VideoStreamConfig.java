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
    
    // 视频流协议配置
    private String videoStreamProtocol; // rtsp, gb28181, or other protocols
    private String rtspTransport; // tcp or udp
    private int rtspTimeout; // RTSP超时时间（秒）
    private String videoCodec; // 视频编码格式，如h264, h265
    private String pixelFormat; // 像素格式，如yuv420p
    private int videoBitrate; // 视频比特率（kbps）
    private int framerate; // 帧率

    /**
     * 从配置文件加载配置
     */
    public static VideoStreamConfig loadConfig() {
        VideoStreamConfig config = new VideoStreamConfig();
        Properties props = new Properties();
        
        try (InputStream input = VideoStreamConfig.class.getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (input == null) {
                LOG.warn("Configuration file 'application.properties' not found in classpath");
                return config;
            }
            
            props.load(input);
            
            // 加载Kafka配置
            config.setKafkaBootstrapServers(props.getProperty("kafka.bootstrap.servers", "localhost:9092"));
            config.setKafkaTopic(props.getProperty("kafka.topic", "video-stream-topic"));
            config.setKafkaGroupId(props.getProperty("kafka.group.id", "video-processing-group"));
            
            // 加载Doris配置
            config.setDorisFenodes(props.getProperty("doris.fenodes", "localhost:8030"));
            config.setDorisDatabase(props.getProperty("doris.database", "video_analytics"));
            config.setDorisTable(props.getProperty("doris.table", "video_detections"));
            config.setDorisUsername(props.getProperty("doris.username", "root"));
            config.setDorisPassword(props.getProperty("doris.password", ""));
            
            // 加载OSS配置
            config.setOssEndpoint(props.getProperty("oss.endpoint", "http://oss-cn-hangzhou.aliyuncs.com"));
            config.setOssAccessKeyId(props.getProperty("oss.access.key.id", ""));
            config.setOssAccessKeySecret(props.getProperty("oss.access.key.secret", ""));
            config.setOssBucket(props.getProperty("oss.bucket", "video-storage"));
            
            // 加载YOLO配置
            config.setYoloModelPath(props.getProperty("yolo.model.path", 
                    "src/main/resources/models/yolov8n.onnx"));
            config.setYoloConfidenceThreshold(Float.parseFloat(
                    props.getProperty("yolo.confidence.threshold", "0.5")));
            
            // 加载视频处理配置
            config.setVideoSegmentDuration(Long.parseLong(
                    props.getProperty("video.segment.duration", "180000"))); // 3分钟
            config.setKeyframeMinInterval(Long.parseLong(
                    props.getProperty("keyframe.min.interval", "5000"))); // 5秒
            
            // 加载视频流协议配置
            config.setVideoStreamProtocol(props.getProperty("video.stream.protocol", "rtsp"));
            config.setRtspTransport(props.getProperty("rtsp.transport", "tcp"));
            config.setRtspTimeout(Integer.parseInt(props.getProperty("rtsp.timeout", "30")));
            config.setVideoCodec(props.getProperty("video.codec", "libx264"));
            config.setPixelFormat(props.getProperty("pixel.format", "yuv420p"));
            config.setVideoBitrate(Integer.parseInt(props.getProperty("video.bitrate", "2048")));
            config.setFramerate(Integer.parseInt(props.getProperty("framerate", "25")));
            
            LOG.info("Configuration loaded successfully");
            LOG.info("Kafka: {} -> topic: {}", config.getKafkaBootstrapServers(), config.getKafkaTopic());
            LOG.info("Doris: {} -> {}.{}", config.getDorisFenodes(), config.getDorisDatabase(), config.getDorisTable());
            LOG.info("Video protocol: {}, RTSP transport: {}, timeout: {}s", 
                    config.getVideoStreamProtocol(), config.getRtspTransport(), config.getRtspTimeout());
            LOG.info("Video codec: {}, bitrate: {}k, framerate: {}", 
                    config.getVideoCodec(), config.getVideoBitrate(), config.getFramerate());
            
        } catch (Exception e) {
            LOG.error("Error loading configuration", e);
            throw new RuntimeException("Failed to load configuration", e);
        }
        
        return config;
    }
}