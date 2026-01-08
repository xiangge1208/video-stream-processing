package com.video.streaming;

import com.video.streaming.config.VideoStreamConfig;
import com.video.streaming.function.VideoProcessFunction;
import com.video.streaming.model.VideoFrame;
import com.video.streaming.serialization.VideoFrameDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 视频流处理主作业
 * <p>
 * 功能特性：
 * 1. 从Kafka接收视频帧数据
 * 2. 实时关键帧提取（基于场景变化和时间间隔）
 * 3. YOLO目标检测
 * 4. 检测结果实时入库Doris
 * 5. 视频片段按3分钟切片存储到OSS
 * <p>
 * 配置参数：
 * - kafka.bootstrap.servers: Kafka服务器地址
 * - doris.fenodes: Doris FE节点地址
 * - oss.endpoint: 对象存储服务地址
 * - video.segment.duration: 视频切片时长（毫秒）
 * - keyframe.min.interval: 关键帧最小间隔（毫秒）
 */
public class VideoStreamProcessingJob {
    
    private static final Logger LOG = LoggerFactory.getLogger(VideoStreamProcessingJob.class);

    public static void main(String[] args) throws Exception {
        // 加载配置
        VideoStreamConfig config = VideoStreamConfig.loadConfig();
        
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 配置环境参数
        configureEnvironment(env);

        // 创建Kafka源
        KafkaSource<VideoFrame> kafkaSource = createKafkaSource(config);

        // 添加数据源
        DataStream<VideoFrame> videoStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.<VideoFrame>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp()),
                "KafkaVideoSource"
        );

        // 应用视频处理函数
        videoStream.process(new VideoProcessFunction(config))
                .name("VideoProcessFunction")
                .uid("video-process-function");

        LOG.info("Starting Video Stream Processing Job...");
        env.execute("Video Stream Processing Job");
    }

    /**
     * 配置Flink执行环境
     */
    private static void configureEnvironment(StreamExecutionEnvironment env) {
        // 设置并行度
        env.setParallelism(4);

        // 开启Checkpoint
        env.enableCheckpointing(60000, org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(600000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 重启次数
                Time.of(10, TimeUnit.SECONDS) // 重启间隔
        ));

        LOG.info("Flink environment configured successfully");
    }

    /**
     * 创建Kafka Source
     */
    private static KafkaSource<VideoFrame> createKafkaSource(VideoStreamConfig config) {
        return KafkaSource.<VideoFrame>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setTopics(config.getKafkaTopic())
                .setGroupId(config.getKafkaGroupId())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new VideoFrameDeserializationSchema())
                .build();
    }
}