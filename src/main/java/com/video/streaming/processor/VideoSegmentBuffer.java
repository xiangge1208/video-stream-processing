package com.video.streaming.processor;

import com.video.streaming.model.VideoFrame;
import com.video.streaming.model.VideoSegment;
import com.video.streaming.util.FFmpegUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 视频切片缓冲器
 * 功能：缓存视频帧并在达到3分钟时生成视频文件
 */
public class VideoSegmentBuffer implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(VideoSegmentBuffer.class);

    private final long segmentDuration; // 切片时长（毫秒）
    private List<VideoFrame> frames;
    private long segmentStartTime;

    public VideoSegmentBuffer(long segmentDuration) {
        this.segmentDuration = segmentDuration;
        this.frames = new ArrayList<>();
        this.segmentStartTime = 0;
    }

    /**
     * 添加视频帧
     */
    public void addFrame(VideoFrame frame) {
        if (frames.isEmpty()) {
            segmentStartTime = frame.getTimestamp();
        }
        frames.add(frame);
    }

    /**
     * 判断是否应该输出视频段
     */
    public boolean shouldFlush(long currentTimestamp) {
        if (frames.isEmpty()) {
            return false;
        }
        return (currentTimestamp - segmentStartTime) >= segmentDuration;
    }

    /**
     * 构建视频段
     */
    public VideoSegment buildSegment(String streamId) {
        if (frames.isEmpty()) {
            return null;
        }

        try {
            long startTime = frames.get(0).getTimestamp();
            long endTime = frames.get(frames.size() - 1).getTimestamp();
            long duration = endTime - startTime;

            // 使用FFmpeg将帧编码为视频文件
            String outputPath = FFmpegUtils.encodeFramesToVideo(
                    frames,
                    streamId,
                    startTime
            );

            if (outputPath == null) {
                LOG.error("Failed to encode video segment for stream: {}", streamId);
                return null;
            }

            // 获取文件大小
            long fileSize = FFmpegUtils.getFileSize(outputPath);

            VideoSegment segment = VideoSegment.builder()
                    .streamId(streamId)
                    .startTime(startTime)
                    .endTime(endTime)
                    .localFilePath(outputPath)
                    .frameCount(frames.size())
                    .fileSize(fileSize)
                    .duration(duration)
                    .build();

            LOG.info("Video segment built: stream={}, frames={}, duration={}ms, size={}bytes",
                    streamId, frames.size(), duration, fileSize);

            return segment;

        } catch (Exception e) {
            LOG.error("Error building video segment for stream: {}", streamId, e);
            return null;
        }
    }

    /**
     * 重置缓冲区
     */
    public void reset(long newStartTime) {
        frames.clear();
        segmentStartTime = newStartTime;
    }

    /**
     * 检查是否有帧
     */
    public boolean hasFrames() {
        return !frames.isEmpty();
    }

    /**
     * 获取帧数量
     */
    public int getFrameCount() {
        return frames.size();
    }

    /**
     * 获取所有帧（用于状态恢复）
     */
    public List<VideoFrame> getFrames() {
        return new ArrayList<>(frames);
    }
}