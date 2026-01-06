package com.video.streaming.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 视频帧数据模型
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class VideoFrame implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 视频流ID（摄像头ID或流标识）
     */
    private String streamId;

    /**
     * 帧ID（全局唯一）
     */
    private Long frameId;

    /**
     * 时间戳（毫秒）
     */
    private Long timestamp;

    /**
     * 帧数据（字节数组，JPEG编码）
     */
    private byte[] frameData;

    /**
     * 帧序号（在当前流中的顺序）
     */
    private Integer frameSequence;

    /**
     * 元数据
     */
    private FrameMetadata metadata;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FrameMetadata implements Serializable {
        private static final long serialVersionUID = 1L;

        /**
         * 分辨率宽度
         */
        private Integer width;

        /**
         * 分辨率高度
         */
        private Integer height;

        /**
         * 帧率
         */
        private Integer fps;

        /**
         * 编码格式
         */
        private String codec;
    }
}