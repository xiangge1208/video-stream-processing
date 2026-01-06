package com.video.streaming.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * 检测结果模型
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DetectionResult implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 视频流ID
     */
    private String streamId;

    /**
     * 帧ID
     */
    private Long frameId;

    /**
     * 检测时间戳
     */
    private Long timestamp;

    /**
     * 关键帧URL（存储在OSS的地址）
     */
    private String frameUrl;

    /**
     * 检测到的目标列表
     */
    private List<Detection> detections;
}