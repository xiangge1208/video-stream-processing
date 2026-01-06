package com.video.streaming.processor;

import ai.onnxruntime.*;
import com.video.streaming.model.Detection;
import com.video.streaming.util.ImageUtils;
import org.opencv.core.Mat;
import org.opencv.core.Size;
import org.opencv.imgproc.Imgproc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * YOLO目标检测器（使用ONNX Runtime）
 */
public class YOLODetector implements Serializable, AutoCloseable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(YOLODetector.class);

    // YOLO模型输入尺寸
    private static final int INPUT_WIDTH = 640;
    private static final int INPUT_HEIGHT = 640;

    // YOLO类别（COCO数据集）
    private static final String[] CLASS_NAMES = {
            "person", "bicycle", "car", "motorcycle", "airplane", "bus", "train", "truck", "boat",
            "traffic light", "fire hydrant", "stop sign", "parking meter", "bench", "bird", "cat",
            "dog", "horse", "sheep", "cow", "elephant", "bear", "zebra", "giraffe", "backpack",
            "umbrella", "handbag", "tie", "suitcase", "frisbee", "skis", "snowboard", "sports ball",
            "kite", "baseball bat", "baseball glove", "skateboard", "surfboard", "tennis racket",
            "bottle", "wine glass", "cup", "fork", "knife", "spoon", "bowl", "banana", "apple",
            "sandwich", "orange", "broccoli", "carrot", "hot dog", "pizza", "donut", "cake", "chair",
            "couch", "potted plant", "bed", "dining table", "toilet", "tv", "laptop", "mouse",
            "remote", "keyboard", "cell phone", "microwave", "oven", "toaster", "sink", "refrigerator",
            "book", "clock", "vase", "scissors", "teddy bear", "hair drier", "toothbrush"
    };

    private transient OrtEnvironment env;
    private transient OrtSession session;
    private final String modelPath;
    private final float confidenceThreshold;

    public YOLODetector(String modelPath, float confidenceThreshold) {
        this.modelPath = modelPath;
        this.confidenceThreshold = confidenceThreshold;
        initialize();
    }

    /**
     * 初始化ONNX Runtime会话
     */
    private void initialize() {
        try {
            env = OrtEnvironment.getEnvironment();
            OrtSession.SessionOptions opts = new OrtSession.SessionOptions();

            // 设置线程数
            opts.setIntraOpNumThreads(2);

            // 加载模型
            session = env.createSession(modelPath, opts);

            LOG.info("YOLO model loaded successfully from: {}", modelPath);
            LOG.info("Model inputs: {}", session.getInputNames());
            LOG.info("Model outputs: {}", session.getOutputNames());

        } catch (Exception e) {
            LOG.error("Failed to initialize YOLO detector", e);
            throw new RuntimeException("YOLO detector initialization failed", e);
        }
    }

    /**
     * 检测目标
     */
    public List<Detection> detect(byte[] imageData) throws OrtException {
        if (session == null) {
            initialize();
        }

        // 1. 预处理图像
        Mat image = ImageUtils.decodeImage(imageData);
        if (image.empty()) {
            LOG.warn("Failed to decode image");
            return new ArrayList<>();
        }

        // 保存原始尺寸用于坐标转换
        int originalWidth = image.cols();
        int originalHeight = image.rows();

        // 调整图像大小
        Mat resized = new Mat();
        Imgproc.resize(image, resized, new Size(INPUT_WIDTH, INPUT_HEIGHT));

        // 转换为RGB并归一化
        float[] inputData = ImageUtils.matToFloatArray(resized);

        // 释放Mat对象
        image.release();
        resized.release();

        // 2. 构建ONNX输入
        long[] shape = {1, 3, INPUT_HEIGHT, INPUT_WIDTH};
        OnnxTensor inputTensor = OnnxTensor.createTensor(
                env,
                FloatBuffer.wrap(inputData),
                shape
        );

        // 3. 执行推理
        OrtSession.Result results = session.run(
                java.util.Collections.singletonMap("images", inputTensor)
        );

        // 4. 解析输出
        float[][] output = (float[][]) results.get(0).getValue();

        // 5. 后处理：NMS非极大值抑制
        List<Detection> detections = postProcess(
                output,
                originalWidth,
                originalHeight
        );

        // 清理资源
        inputTensor.close();
        results.close();

        return detections;
    }

    /**
     * 后处理检测结果
     */
    private List<Detection> postProcess(float[][] output, int originalWidth, int originalHeight) {
        List<Detection> detections = new ArrayList<>();

        // YOLOv8输出格式: [batch, 84, 8400]
        // 84 = 4(bbox) + 80(classes)
        int numDetections = output[0].length / 84;

        for (int i = 0; i < numDetections; i++) {
            int offset = i * 84;

            // 获取边界框坐标 (center_x, center_y, width, height)
            float cx = output[0][offset];
            float cy = output[0][offset + 1];
            float w = output[0][offset + 2];
            float h = output[0][offset + 3];

            // 找到最大置信度的类别
            int maxClassIdx = 0;
            float maxConfidence = 0;

            for (int j = 0; j < 80; j++) {
                float confidence = output[0][offset + 4 + j];
                if (confidence > maxConfidence) {
                    maxConfidence = confidence;
                    maxClassIdx = j;
                }
            }

            // 过滤低置信度检测
            if (maxConfidence < confidenceThreshold) {
                continue;
            }

            // 转换坐标到原始图像尺寸
            float scaleX = (float) originalWidth / INPUT_WIDTH;
            float scaleY = (float) originalHeight / INPUT_HEIGHT;

            float x1 = (cx - w / 2) * scaleX;
            float y1 = (cy - h / 2) * scaleY;
            float x2 = (cx + w / 2) * scaleX;
            float y2 = (cy + h / 2) * scaleY;

            // 创建检测结果
            Detection detection = Detection.builder()
                    .objectClass(CLASS_NAMES[maxClassIdx])
                    .confidence(maxConfidence)
                    .bbox(Detection.BoundingBox.builder()
                            .x1(x1)
                            .y1(y1)
                            .x2(x2)
                            .y2(y2)
                            .build())
                    .build();

            detections.add(detection);
        }

        // 应用NMS去除重叠的框
        return applyNMS(detections, 0.45f);
    }

    /**
     * 非极大值抑制
     */
    private List<Detection> applyNMS(List<Detection> detections, float iouThreshold) {
        if (detections.isEmpty()) {
            return detections;
        }

        // 按置信度排序
        detections.sort((a, b) -> Float.compare(b.getConfidence(), a.getConfidence()));

        List<Detection> result = new ArrayList<>();
        boolean[] suppressed = new boolean[detections.size()];

        for (int i = 0; i < detections.size(); i++) {
            if (suppressed[i]) continue;

            Detection detA = detections.get(i);
            result.add(detA);

            for (int j = i + 1; j < detections.size(); j++) {
                if (suppressed[j]) continue;

                Detection detB = detections.get(j);

                // 计算IOU
                float iou = calculateIOU(detA.getBbox(), detB.getBbox());

                if (iou > iouThreshold) {
                    suppressed[j] = true;
                }
            }
        }

        return result;
    }

    /**
     * 计算两个边界框的IOU
     */
    private float calculateIOU(Detection.BoundingBox box1, Detection.BoundingBox box2) {
        float x1 = Math.max(box1.getX1(), box2.getX1());
        float y1 = Math.max(box1.getY1(), box2.getY1());
        float x2 = Math.min(box1.getX2(), box2.getX2());
        float y2 = Math.min(box1.getY2(), box2.getY2());

        float intersectionArea = Math.max(0, x2 - x1) * Math.max(0, y2 - y1);

        float box1Area = (box1.getX2() - box1.getX1()) * (box1.getY2() - box1.getY1());
        float box2Area = (box2.getX2() - box2.getX1()) * (box2.getY2() - box2.getY1());

        float unionArea = box1Area + box2Area - intersectionArea;

        return intersectionArea / unionArea;
    }

    @Override
    public void close() {
        try {
            if (session != null) {
                session.close();
            }
            if (env != null) {
                env.close();
            }
            LOG.info("YOLO detector closed");
        } catch (Exception e) {
            LOG.error("Error closing YOLO detector", e);
        }
    }
}