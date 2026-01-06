package com.video.streaming.function;

import com.video.streaming.VideoStreamProcessingJob;
import com.video.streaming.model.DetectionResult;
import com.video.streaming.model.VideoFrame;
import com.video.streaming.model.VideoSegment;
import com.video.streaming.processor.KeyFrameExtractor;
import com.video.streaming.processor.VideoSegmentBuffer;
import com.video.streaming.processor.YOLODetector;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 视频处理核心函数
 * 功能：
 * 1. 关键帧提取
 * 2. YOLO目标检测
 * 3. 视频切片缓冲
 */
public class VideoProcessFunction
        extends KeyedProcessFunction<String, VideoFrame, DetectionResult>
        implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(VideoProcessFunction.class);

    // 3分钟切片时长（毫秒）
    private static final long SEGMENT_DURATION_MS = 3 * 60 * 1000;

    // 处理器
    private transient KeyFrameExtractor keyFrameExtractor;
    private transient YOLODetector yoloDetector;
    private transient VideoSegmentBuffer segmentBuffer;

    // 状态：保存未完成的视频段
    private transient ListState<VideoFrame> videoBufferState;

    // 指标计数器
    private long totalFramesProcessed = 0;
    private long keyFramesExtracted = 0;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        LOG.info("Initializing VideoProcessFunction...");

        // 初始化关键帧提取器
        keyFrameExtractor = new KeyFrameExtractor();
        LOG.info("KeyFrameExtractor initialized");

        // 初始化YOLO检测器
        String modelPath = parameters.getString("yolo.model.path",
                "D:\\workspace\\video-stream-processing\\src\\main\\resources\\models\\yolov8n.onnx");
        float confidenceThreshold = parameters.getFloat("yolo.confidence.threshold", 0.5f);
        yoloDetector = new YOLODetector(modelPath, confidenceThreshold);
        LOG.info("YOLODetector initialized with model: {}", modelPath);

        // 初始化视频段缓冲器
        segmentBuffer = new VideoSegmentBuffer(SEGMENT_DURATION_MS);
        LOG.info("VideoSegmentBuffer initialized with duration: {}ms", SEGMENT_DURATION_MS);
    }

    @Override
    public void processElement(VideoFrame frame,
                               Context ctx,
                               Collector<DetectionResult> out) throws Exception {

        String streamId = frame.getStreamId();
        long timestamp = frame.getTimestamp();

        totalFramesProcessed++;

        // 1. 添加帧到视频段缓冲区
        segmentBuffer.addFrame(frame);

        // 2. 检查是否需要输出视频段
        if (segmentBuffer.shouldFlush(timestamp)) {
            VideoSegment segment = segmentBuffer.buildSegment(streamId);
            if (segment != null) {
                // 输出到侧输出流
                ctx.output(VideoStreamProcessingJob.VIDEO_SEGMENT_TAG, segment);
                LOG.info("Video segment created for stream: {}, frames: {}, duration: {}ms",
                        streamId, segment.getFrameCount(), segment.getDuration());
            }
            segmentBuffer.reset(timestamp);
        }

        // 3. 关键帧检测
        if (keyFrameExtractor.isKeyFrame(frame)) {
            keyFramesExtracted++;

            try {
                // 4. YOLO目标检测
                List<com.video.streaming.model.Detection> detections =
                        yoloDetector.detect(frame.getFrameData());

                // 5. 构建检测结果
                DetectionResult result = DetectionResult.builder()
                        .streamId(streamId)
                        .frameId(frame.getFrameId())
                        .timestamp(timestamp)
                        .detections(detections)
                        .frameUrl(null) // 可选：上传关键帧到OSS并设置URL
                        .build();

                // 6. 输出检测结果
                out.collect(result);

                if (detections != null && !detections.isEmpty()) {
                    LOG.debug("Detected {} objects in frame {} from stream {}",
                            detections.size(), frame.getFrameId(), streamId);
                }
            } catch (Exception e) {
                LOG.error("Error detecting objects in frame {} from stream {}",
                        frame.getFrameId(), streamId, e);
            }
        }

        // 定期打印处理统计信息
        if (totalFramesProcessed % 1000 == 0) {
            LOG.info("Stream: {}, Total frames: {}, Key frames: {}, Rate: {:.2f}%",
                    streamId, totalFramesProcessed, keyFramesExtracted,
                    (keyFramesExtracted * 100.0 / totalFramesProcessed));
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
                totalFramesProcessed, keyFramesExtracted);
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