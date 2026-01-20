package com.video.streaming;

import com.video.streaming.config.VideoStreamConfig;
import com.video.streaming.function.VideoProcessFunction;
import com.video.streaming.function.VideoSegmentFunction;
import com.video.streaming.model.DetectionResult;
import com.video.streaming.model.VideoFrame;
import com.video.streaming.model.VideoSegment;
import com.video.streaming.serialization.VideoFrameDeserializationSchema;
import com.video.streaming.sink.DorisSinkBuilder;
import com.video.streaming.sink.MinIOVideoSink;
import com.video.streaming.sink.OSSVideoSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * 视频流处理主任务
 * 功能：
 * 1. 从Kafka接收视频流数据
 * 2. 提取关键帧并进行YOLO目标检测
 * 3. 将检测结果写入Doris
 * 4. 按3分钟切片保存原视频到OSS
 */
public class VideoStreamProcessingJob {

    private static final Logger LOG = LoggerFactory.getLogger(VideoStreamProcessingJob.class);

    // 侧输出标签 - 用于视频切片输出
    public static final OutputTag<VideoSegment> VIDEO_SEGMENT_TAG =
            new OutputTag<VideoSegment>("video-segment"){};

    public static void main(String[] args) throws Exception {

        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 配置环境参数
        configureEnvironment(env);

        // 3. 加载配置
        VideoStreamConfig config = VideoStreamConfig.loadConfig();

        // 4. 创建Kafka Source
        KafkaSource<VideoFrame> kafkaSource = createKafkaSource(config);

        // 5. 从Kafka读取视频流数据
        DataStream<VideoFrame> videoStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka-Video-Source")
                .name("video-source")
                .uid("video-source-uid");

        // 6. 视频处理 - 关键帧提取 + YOLO检测
        SingleOutputStreamOperator<DetectionResult> detectionStream = videoStream
                .keyBy(VideoFrame::getStreamId)
                .process(new VideoProcessFunction())
                .name("video-processing")
                .uid("video-processing-uid");

        // 7. 获取视频切片侧输出流
        DataStream<VideoSegment> videoSegmentStream = detectionStream
                .getSideOutput(VIDEO_SEGMENT_TAG);

        // 8. 检测结果转换为JSON并写入Doris
        DataStream<String> jsonStringStream = detectionStream
                .map(DorisSinkBuilder::convertToJson) // 将DetectionResult转换为JSON字符串
                .filter(Objects::nonNull); // 过滤空JSON

        DorisSinkBuilder.addToDoris(jsonStringStream, config);

        // 9. 视频切片保存到对象存储 (OSS 或 MinIO)
        String storageType = config.getStorageType();
        if ("minio".equalsIgnoreCase(storageType)) {
            videoSegmentStream
                    .keyBy(VideoSegment::getStreamId)
                    .process(new VideoSegmentFunction())
                    .addSink(new MinIOVideoSink(config))
                    .name("minio-sink")
                    .uid("minio-sink-uid");
            LOG.info("Using MinIO for video storage");
        } else {
            // 默认使用 OSS
            videoSegmentStream
                    .keyBy(VideoSegment::getStreamId)
                    .process(new VideoSegmentFunction())
                    .addSink(new OSSVideoSink(config))
                    .name("oss-sink")
                    .uid("oss-sink-uid");
            LOG.info("Using OSS for video storage");
        }

        // 10. 执行任务
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
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
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