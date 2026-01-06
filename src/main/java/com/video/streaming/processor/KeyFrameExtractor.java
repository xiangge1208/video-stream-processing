package com.video.streaming.processor;

import com.video.streaming.model.VideoFrame;
import com.video.streaming.util.ImageUtils;
import org.opencv.core.Mat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * 关键帧提取器
 * 策略：
 * 1. 时间间隔策略：每5秒至少一个关键帧
 * 2. 场景变化策略：检测场景切换
 * 3. 运动显著性策略：检测显著运动
 */
public class KeyFrameExtractor implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(KeyFrameExtractor.class);

    // 最小关键帧间隔（毫秒）
    private static final long MIN_KEYFRAME_INTERVAL = 5000; // 5秒

    // 场景变化阈值
    private static final double SCENE_CHANGE_THRESHOLD = 0.3;

    // 上一帧信息
    private transient VideoFrame previousFrame;
    private transient long lastKeyFrameTime = 0;

    /**
     * 判断是否为关键帧
     */
    public boolean isKeyFrame(VideoFrame currentFrame) {
        long currentTime = currentFrame.getTimestamp();

        // 策略1: 时间间隔检查（强制策略）
        if (currentTime - lastKeyFrameTime >= MIN_KEYFRAME_INTERVAL) {
            lastKeyFrameTime = currentTime;
            previousFrame = currentFrame;
            return true;
        }

        // 策略2: 场景变化检测（如果有前一帧）
        if (previousFrame != null) {
            try {
                double similarity = calculateFrameSimilarity(previousFrame, currentFrame);

                // 如果相似度低于阈值，认为是场景变化
                if (similarity < (1 - SCENE_CHANGE_THRESHOLD)) {
                    LOG.debug("Scene change detected, similarity: {}", similarity);
                    lastKeyFrameTime = currentTime;
                    previousFrame = currentFrame;
                    return true;
                }
            } catch (Exception e) {
                LOG.warn("Error calculating frame similarity: {}", e.getMessage());
            }
        }

        previousFrame = currentFrame;
        return false;
    }

    /**
     * 计算两帧之间的相似度（使用直方图比较）
     */
    private double calculateFrameSimilarity(VideoFrame frame1, VideoFrame frame2) {
        try {
            // 解码图像
            Mat mat1 = ImageUtils.decodeImage(frame1.getFrameData());
            Mat mat2 = ImageUtils.decodeImage(frame2.getFrameData());

            if (mat1.empty() || mat2.empty()) {
                return 1.0; // 解码失败，假设相似
            }

            // 计算直方图相似度
            double similarity = ImageUtils.compareHistograms(mat1, mat2);

            // 释放资源
            mat1.release();
            mat2.release();

            return similarity;
        } catch (Exception e) {
            LOG.warn("Error in similarity calculation: {}", e.getMessage());
            return 1.0; // 出错时假设相似
        }
    }

    /**
     * 重置状态
     */
    public void reset() {
        previousFrame = null;
        lastKeyFrameTime = 0;
    }
}