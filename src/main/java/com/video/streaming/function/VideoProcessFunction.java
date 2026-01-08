package com.video.streaming.function;

import com.video.streaming.config.VideoStreamConfig;
import com.video.streaming.model.Detection;
import com.video.streaming.model.DetectionResult;
import com.video.streaming.model.VideoFrame;
import com.video.streaming.model.VideoSegment;
import com.video.streaming.processor.KeyFrameExtractor;
import com.video.streaming.processor.VideoSegmentBuffer;
import com.video.streaming.processor.YOLODetector;
import com.video.streaming.sink.DorisSinkBuilder;
import com.video.streaming.util.ImageUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.bytedeco.opencv.opencv_core.Mat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 视频处理函数
 * 功能：
 * 1. 关键帧提取
 * 2. YOLO目标检测
 * 3. 检测结果发送到Doris
 * 4. 视频按3分钟切片保存
 */
public class VideoProcessFunction extends ProcessFunction<VideoFrame, String> implements CheckpointedFunction {
    private static final Logger LOG = LoggerFactory.getLogger(VideoProcessFunction.class);

    // 状态管理
    private transient ListState<VideoFrame> videoBufferState;
    private transient ValueState<String> lastProcessedStreamIdState;

    // 处理组件
    private transient YOLODetector yoloDetector;
    private transient KeyFrameExtractor keyFrameExtractor;
    private transient VideoSegmentBuffer segmentBuffer;
    private VideoStreamConfig config;

    // 统计信息
    private transient AtomicLong totalFramesProcessed;
    private transient AtomicLong keyFramesExtracted;

    public VideoProcessFunction(VideoStreamConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 初始化处理组件
        yoloDetector = new YOLODetector(
                config.getYoloModelPath(),
                config.getYoloConfidenceThreshold()
        );
        keyFrameExtractor = new KeyFrameExtractor(config.getKeyframeMinInterval());
        
        // 使用配置参数创建视频切片缓冲区
        segmentBuffer = new VideoSegmentBuffer(config.getVideoSegmentDuration(), config);

        // 初始化统计计数器
        totalFramesProcessed = new AtomicLong(0);
        keyFramesExtracted = new AtomicLong(0);

        LOG.info("VideoProcessFunction opened and initialized");
    }

    @Override
    public void processElement(VideoFrame frame, Context ctx, Collector<String> out) throws Exception {
        String streamId = frame.getStreamId();
        long currentTimestamp = System.currentTimeMillis();

        // 更新统计信息
        long totalProcessed = totalFramesProcessed.incrementAndGet();

        // 关键帧检测
        boolean isKeyFrame = keyFrameExtractor.isKeyFrame(frame);
        if (isKeyFrame) {
            keyFramesExtracted.incrementAndGet();

            try {
                // 执行YOLO检测
                List<Detection> detections = yoloDetector.detect(frame.getFrameData());

                // 处理检测结果
                if (!detections.isEmpty()) {
                    // 创建检测结果对象
                    DetectionResult result = DetectionResult.builder()
                            .streamId(streamId)
                            .timestamp(frame.getTimestamp())
                            .frameId(frame.getFrameId())
                            .detections(detections)
                            .frameUrl(null) // 暂时为空，可根据需要填充
                            .build();

                    // 将检测结果转换为JSON并发送到Doris
                    String jsonResult = DorisSinkBuilder.convertToJson(result);
                    if (jsonResult != null) {
                        // 这里应该有实际的数据流来发送到Doris
                        LOG.debug("Detection result: stream={}, frame={}, detections={}",
                                streamId, frame.getFrameId(), detections.size());
                    }
                }

                // 将关键帧添加到切片缓冲区
                segmentBuffer.addFrame(frame);

                // 检查是否需要刷新视频切片
                if (segmentBuffer.shouldFlush(currentTimestamp)) {
                    VideoSegment segment = segmentBuffer.buildSegment(streamId);
                    if (segment != null) {
                        LOG.info("Video segment flushed: {}", segment.getLocalFilePath());
                    }
                }

            } catch (Exception e) {
                LOG.error("Error processing key frame for stream {}, frame {}", 
                        streamId, frame.getFrameId(), e);
            }
        } else {
            // 对于非关键帧，仍然添加到切片缓冲区以维持连续性
            segmentBuffer.addFrame(frame);

            // 检查是否需要刷新视频切片
            if (segmentBuffer.shouldFlush(currentTimestamp)) {
                VideoSegment segment = segmentBuffer.buildSegment(streamId);
                if (segment != null) {
                    LOG.info("Video segment flushed: {}", segment.getLocalFilePath());
                }
            }
        }

        // 定期打印处理统计信息
        if (totalProcessed % 1000 == 0) {
            LOG.info("Stream: {}, Total frames: {}, Key frames: {}, Rate: {:.2f}%",
                    streamId, totalProcessed, keyFramesExtracted.get(),
                    (keyFramesExtracted.get() * 100.0 / totalProcessed));
        }
    }

    @Override
    public void close() throws Exception {
        super.close();

        // 关闭资源
        if (yoloDetector != null) {
            yoloDetector.close();
        }

        LOG.info("VideoProcessFunction closed. Total frames processed: {}, Key frames: {}",
                totalFramesProcessed.get(), keyFramesExtracted.get());
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // 保存状态到Checkpoint
        videoBufferState.clear();
        if (segmentBuffer != null && segmentBuffer.hasFrames()) {
            for (VideoFrame frame : segmentBuffer.getFrames()) {
                videoBufferState.add(frame);
            }
        }
        LOG.debug("State snapshot saved, frames: {}",
                segmentBuffer != null ? segmentBuffer.getFrameCount() : 0);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 初始化状态
        ListStateDescriptor<VideoFrame> descriptor = new ListStateDescriptor<>(
                "video-buffer-state",
                VideoFrame.class
        );
        videoBufferState = context.getOperatorStateStore().getListState(descriptor);

        // 恢复状态（如果有）
        if (context.isRestored()) {
            List<VideoFrame> restoredFrames = new ArrayList<>();
            for (VideoFrame frame : videoBufferState.get()) {
                restoredFrames.add(frame);
            }
            LOG.info("State restored, frames recovered: {}", restoredFrames.size());

            // 重建缓冲区
            if (!restoredFrames.isEmpty() && segmentBuffer != null) {
                for (VideoFrame frame : restoredFrames) {
                    segmentBuffer.addFrame(frame);
                }
            }
        }
    }
}