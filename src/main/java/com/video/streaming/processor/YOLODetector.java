package com.video.streaming.processor;

import ai.onnxruntime.*;
import com.video.streaming.model.Detection;
import com.video.streaming.util.ImageUtils;
import org.bytedeco.opencv.opencv_core.Mat;
import org.bytedeco.opencv.global.opencv_imgproc;
import org.bytedeco.opencv.opencv_core.Size;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * YOLOç›®æ ‡æ£€æµ‹å™¨ï¼ˆä½¿ç"¨ONNX Runtimeï¼‰
 * æ"¯æŒ YOLOv8 æ¨¡åž‹æ ¼å¼: [1, 84, 8400]
 *
 * 修复内容：
 * 1. 正确的资源释放顺序
 * 2. 线程安全的推理
 * 3. 完善的异常处理
 */
public class YOLODetector implements Serializable, AutoCloseable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(YOLODetector.class);

    private static final int INPUT_WIDTH = 640;
    private static final int INPUT_HEIGHT = 640;

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
    private final float iouThreshold;

    // 添加同步锁以确保线程安全
    private final Object lock = new Object();

    public YOLODetector(String modelPath, float confidenceThreshold) {
        this(modelPath, confidenceThreshold, 0.45f);
    }

    public YOLODetector(String modelPath, float confidenceThreshold, float iouThreshold) {
        this.modelPath = modelPath;
        this.confidenceThreshold = confidenceThreshold;
        this.iouThreshold = iouThreshold;
        initialize();
    }

    private void initialize() {
        try {
            env = OrtEnvironment.getEnvironment();
            OrtSession.SessionOptions opts = new OrtSession.SessionOptions();
            opts.setIntraOpNumThreads(2);

            // 设置优化级别
            opts.setOptimizationLevel(OrtSession.SessionOptions.OptLevel.BASIC_OPT);

            if (env != null) {
                session = env.createSession(modelPath, opts);
                LOG.info("YOLO model loaded successfully from: {}", modelPath);
                LOG.info("Model inputs: {}", session.getInputNames());
                LOG.info("Model outputs: {}", session.getOutputNames());
            }
        } catch (Exception e) {
            LOG.error("Failed to initialize YOLO detector", e);
            throw new RuntimeException("YOLO detector initialization failed", e);
        }
    }

    /**
     * 线程安全的检测方法
     */
    public List<Detection> detect(byte[] imageData) throws OrtException {
        synchronized (lock) {
            return detectInternal(imageData);
        }
    }

    /**
     * 内部检测实现
     */
    private List<Detection> detectInternal(byte[] imageData) throws OrtException {
        if (session == null) {
            initialize();
        }

        Mat image = null;
        Mat resized = null;
        OnnxTensor inputTensor = null;
        OrtSession.Result results = null;

        try {
            // 1. 预处理图像
            image = ImageUtils.decodeImage(imageData);
            if (image == null || image.empty()) {
                LOG.warn("Failed to decode image");
                return new ArrayList<>();
            }

            int originalWidth = image.cols();
            int originalHeight = image.rows();

            resized = new Mat();
            opencv_imgproc.resize(image, resized, new Size(INPUT_WIDTH, INPUT_HEIGHT));
            float[] inputData = ImageUtils.matToFloatArray(resized);

            // 2. 构建ONNX输入
            long[] shape = {1, 3, INPUT_HEIGHT, INPUT_WIDTH};
            inputTensor = OnnxTensor.createTensor(
                    env,
                    FloatBuffer.wrap(inputData),
                    shape
            );

            // 3. 执行推理
            long startTime = System.currentTimeMillis();
            results = session.run(
                    java.util.Collections.singletonMap("images", inputTensor)
            );
            long inferenceTime = System.currentTimeMillis() - startTime;

            // 4. 解析输出 - 在关闭resources之前完成所有数据提取
            OnnxTensor outputTensor = (OnnxTensor) results.get(0);
            long[] outputShape = outputTensor.getInfo().getShape();

            LOG.debug("Output shape: {}, Inference time: {} ms",
                    Arrays.toString(outputShape), inferenceTime);

            Object outputObj = outputTensor.getValue();

            // 提取数据到内存中
            float[][] output = extractOutputData(outputObj);

            if (output == null) {
                LOG.warn("Failed to extract output data");
                return new ArrayList<>();
            }

            // 5. 后处理 - 使用已提取的数据
            List<Detection> detections = postProcess(output, originalWidth, originalHeight);

            LOG.info("Detected {} objects in {} ms", detections.size(), inferenceTime);

            return detections;

        } catch (Exception e) {
            LOG.error("Error during object detection", e);
            throw new OrtException("Detection failed");
        } finally {
            // 正确的资源释放顺序：先释放引用，后释放容器
            safeRelease(resized, "resized Mat");
            safeRelease(image, "image Mat");
            safeClose(results, "results");  // 先关闭results
            safeClose(inputTensor, "inputTensor");  // 再关闭tensor
        }
    }

    /**
     * 提取输出数据到内存中
     */
    private float[][] extractOutputData(Object outputObj) {
        try {
            if (outputObj instanceof float[][][]) {
                float[][][] output3D = (float[][][]) outputObj;
                if (output3D.length > 0) {
                    // 深拷贝数据，避免引用ONNX内部内存
                    float[][] result = new float[output3D[0].length][];
                    for (int i = 0; i < output3D[0].length; i++) {
                        result[i] = Arrays.copyOf(output3D[0][i], output3D[0][i].length);
                    }
                    return result;
                }
            } else if (outputObj instanceof float[][]) {
                float[][] output2D = (float[][]) outputObj;
                // 深拷贝
                float[][] result = new float[output2D.length][];
                for (int i = 0; i < output2D.length; i++) {
                    result[i] = Arrays.copyOf(output2D[i], output2D[i].length);
                }
                return result;
            }
            LOG.error("Unexpected output type: {}", outputObj.getClass().getSimpleName());
        } catch (Exception e) {
            LOG.error("Error extracting output data", e);
        }
        return null;
    }

    /**
     * 安全关闭资源
     */
    private void safeClose(AutoCloseable resource, String name) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception e) {
                LOG.error("Error closing {}", name, e);
            }
        }
    }

    /**
     * 安全释放Mat
     */
    private void safeRelease(Mat mat, String name) {
        if (mat != null && !mat.isNull()) {
            try {
                mat.release();
            } catch (Exception e) {
                LOG.error("Error releasing {}", name, e);
            }
        }
    }

    private List<Detection> postProcess(float[][] output, int originalWidth, int originalHeight) {
        List<Detection> detections = new ArrayList<>();

        if (output == null || output.length < 84) {
            LOG.warn("Invalid output data");
            return detections;
        }

        int numDetections = output[0].length;

        for (int i = 0; i < numDetections; i++) {
            float cx = output[0][i];
            float cy = output[1][i];
            float w = output[2][i];
            float h = output[3][i];

            int maxClassIdx = 0;
            float maxConfidence = output[4][i];

            for (int j = 5; j < 84; j++) {
                float confidence = output[j][i];
                if (confidence > maxConfidence) {
                    maxConfidence = confidence;
                    maxClassIdx = j - 4;
                }
            }

            if (maxConfidence < confidenceThreshold) {
                continue;
            }

            float scaleX = (float) originalWidth / INPUT_WIDTH;
            float scaleY = (float) originalHeight / INPUT_HEIGHT;

            float x1 = Math.max(0, Math.min((cx - w / 2) * scaleX, originalWidth));
            float y1 = Math.max(0, Math.min((cy - h / 2) * scaleY, originalHeight));
            float x2 = Math.max(0, Math.min((cx + w / 2) * scaleX, originalWidth));
            float y2 = Math.max(0, Math.min((cy + h / 2) * scaleY, originalHeight));

            if (x2 <= x1 || y2 <= y1) {
                continue;
            }

            detections.add(Detection.builder()
                    .objectClass(CLASS_NAMES[maxClassIdx])
                    .confidence(maxConfidence)
                    .bbox(Detection.BoundingBox.builder()
                            .x1(x1).y1(y1).x2(x2).y2(y2)
                            .build())
                    .build());
        }

        return applyNMS(detections, iouThreshold);
    }

    private List<Detection> applyNMS(List<Detection> detections, float iouThreshold) {
        if (detections.isEmpty()) {
            return detections;
        }

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

                if (!detA.getObjectClass().equals(detB.getObjectClass())) {
                    continue;
                }

                float iou = calculateIOU(detA.getBbox(), detB.getBbox());
                if (iou > iouThreshold) {
                    suppressed[j] = true;
                }
            }
        }

        return result;
    }

    private float calculateIOU(Detection.BoundingBox box1, Detection.BoundingBox box2) {
        float x1 = Math.max(box1.getX1(), box2.getX1());
        float y1 = Math.max(box1.getY1(), box2.getY1());
        float x2 = Math.min(box1.getX2(), box2.getX2());
        float y2 = Math.min(box1.getY2(), box2.getY2());

        float intersectionArea = Math.max(0, x2 - x1) * Math.max(0, y2 - y1);
        float box1Area = (box1.getX2() - box1.getX1()) * (box1.getY2() - box1.getY1());
        float box2Area = (box2.getX2() - box2.getX1()) * (box2.getY2() - box2.getY1());
        float unionArea = box1Area + box2Area - intersectionArea;

        return unionArea > 0 ? intersectionArea / unionArea : 0;
    }

    @Override
    public void close() {
        synchronized (lock) {
            try {
                if (session != null) {
                    session.close();
                    session = null;
                }
                LOG.info("YOLO detector closed");
            } catch (Exception e) {
                LOG.error("Error closing YOLO detector", e);
            }
        }
    }

    private void readObject(java.io.ObjectInputStream in)
            throws java.io.IOException, ClassNotFoundException {
        in.defaultReadObject();
        initialize();
    }
}