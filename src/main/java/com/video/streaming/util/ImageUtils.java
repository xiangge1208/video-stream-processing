package com.video.streaming.util;

import org.opencv.core.*;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.imgproc.Imgproc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 图像处理工具类
 */
public class ImageUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ImageUtils.class);

    static {
        // 加载OpenCV本地库
        try {
            nu.pattern.OpenCV.loadLocally();
            LOG.info("OpenCV loaded successfully");
        } catch (Exception e) {
            LOG.error("Failed to load OpenCV", e);
        }
    }

    /**
     * 解码图像字节数组为Mat对象
     */
    public static Mat decodeImage(byte[] imageData) {
        MatOfByte matOfByte = new MatOfByte(imageData);
        Mat mat = Imgcodecs.imdecode(matOfByte, Imgcodecs.IMREAD_COLOR);
        matOfByte.release();
        return mat;
    }

    /**
     * 将Mat转换为float数组（用于模型输入）
     */
    public static float[] matToFloatArray(Mat mat) {
        // 转换为RGB
        Mat rgb = new Mat();
        Imgproc.cvtColor(mat, rgb, Imgproc.COLOR_BGR2RGB);

        // 归一化到[0,1]并转换为CHW格式
        int channels = rgb.channels();
        int height = rgb.rows();
        int width = rgb.cols();

        float[] result = new float[channels * height * width];
        byte[] data = new byte[(int) rgb.total() * channels];
        rgb.get(0, 0, data);

        for (int c = 0; c < channels; c++) {
            for (int h = 0; h < height; h++) {
                for (int w = 0; w < width; w++) {
                    int pixelIndex = (h * width + w) * channels + c;
                    int resultIndex = c * height * width + h * width + w;
                    result[resultIndex] = (data[pixelIndex] & 0xFF) / 255.0f;
                }
            }
        }

        rgb.release();
        return result;
    }

    /**
     * 比较两个图像的直方图相似度
     */
    public static double compareHistograms(Mat image1, Mat image2) {
        // 转换为HSV
        Mat hsv1 = new Mat();
        Mat hsv2 = new Mat();
        Imgproc.cvtColor(image1, hsv1, Imgproc.COLOR_BGR2HSV);
        Imgproc.cvtColor(image2, hsv2, Imgproc.COLOR_BGR2HSV);

        // 计算直方图
        Mat hist1 = new Mat();
        Mat hist2 = new Mat();

        MatOfInt histSize = new MatOfInt(50, 60);
        MatOfFloat ranges = new MatOfFloat(0, 180, 0, 256);
        MatOfInt channels = new MatOfInt(0, 1);

        Imgproc.calcHist(
                java.util.Collections.singletonList(hsv1),
                channels, new Mat(), hist1, histSize, ranges
        );

        Imgproc.calcHist(
                java.util.Collections.singletonList(hsv2),
                channels, new Mat(), hist2, histSize, ranges
        );

        // 归一化
        Core.normalize(hist1, hist1, 0, 1, Core.NORM_MINMAX);
        Core.normalize(hist2, hist2, 0, 1, Core.NORM_MINMAX);

        // 比较直方图（使用相关系数法）
        double similarity = Imgproc.compareHist(hist1, hist2, Imgproc.HISTCMP_CORREL);

        // 释放资源
        hsv1.release();
        hsv2.release();
        hist1.release();
        hist2.release();

        return similarity;
    }
}