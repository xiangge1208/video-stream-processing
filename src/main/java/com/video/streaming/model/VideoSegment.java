package com.video.streaming.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 视频切片模型（3分钟）
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class VideoSegment implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 视频流ID
     */
    private String streamId;

    /**
     * 切片开始时间戳
     */
    private Long startTime;

    /**
     * 切片结束时间戳
     */
    private Long endTime;

    /**
     * 本地临时文件路径
     */
    private String localFilePath;

    /**
     * 帧数量
     */
    private Integer frameCount;

    /**
     * 文件大小（字节）
     */
    private Long fileSize;

    /**
     * 视频时长（毫秒）
     */
    private Long duration;
}