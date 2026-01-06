package com.video.streaming.model;

import lombok.*;

import java.io.Serializable;

/**
 * 单个目标检测结果
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Detection implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 目标类别（如：person, car, dog等）
     */
    private String objectClass;

    /**
     * 置信度 (0-1)
     */
    private Float confidence;

    /**
     * 边界框坐标
     */
    private BoundingBox bbox;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BoundingBox implements Serializable {
        private static final long serialVersionUID = 1L;

        private Float x1;  // 左上角x
        private Float y1;  // 左上角y
        private Float x2;  // 右下角x
        private Float y2;  // 右下角y
    }
}