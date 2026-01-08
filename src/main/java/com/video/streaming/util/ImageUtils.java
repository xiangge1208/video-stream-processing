package com.video.streaming.util;

import org.bytedeco.opencv.opencv_core.Mat;
import org.bytedeco.opencv.global.opencv_imgcodecs;
import org.bytedeco.opencv.global.opencv_imgproc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 图像处理工具类
 */
public class ImageUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ImageUtils.class);

    static {
        LOG.info("JavaCV OpenCV loaded successfully");
    }

    /**
     * 解码图像字节数组为Mat对象
     */
    public static Mat decodeImage(byte[] imageData) {
        // 创建一个Mat对象并填充图像数据
        Mat imgBuf = new Mat(1, imageData.length, org.bytedeco.opencv.global.opencv_core.CV_8UC1);
        try {
            imgBuf.data().put(imageData);
            
            // 解码图像
            Mat mat = opencv_imgcodecs.imdecode(imgBuf, opencv_imgcodecs.IMREAD_COLOR);
            return mat;
        } finally {
            // 确保imgBuf被释放
            imgBuf.release();
        }
    }

    /**
     * 将Mat转换为float数组（用于模型输入）
     */
    public static float[] matToFloatArray(Mat mat) {
        // 转换为RGB
        Mat rgb = new Mat();
        try {
            opencv_imgproc.cvtColor(mat, rgb, opencv_imgproc.COLOR_BGR2RGB);

            // 获取图像尺寸
            int channels = (int) rgb.channels();
            int height = (int) rgb.rows();
            int width = (int) rgb.cols();

            // 读取像素数据
            int totalPixels = height * width * channels;
            byte[] data = new byte[totalPixels];
            rgb.data().get(data);

            // 转换为CHW格式的浮点数组并归一化到[0,1]
            float[] result = new float[totalPixels];
            for (int c = 0; c < channels; c++) {
                for (int h = 0; h < height; h++) {
                    for (int w = 0; w < width; w++) {
                        int pixelIndex = (h * width + w) * channels + c;
                        int resultIndex = c * height * width + h * width + w;
                        result[resultIndex] = ((data[pixelIndex] & 0xFF) / 255.0f);
                    }
                }
            }

            return result;
        } finally {
            // 确保Mat对象被释放
            rgb.release();
        }
    }

    /**
     * 比较两个图像的直方图相似度
     * 注意：由于复杂性，暂时简化实现
     */
    public static double compareHistograms(Mat image1, Mat image2) {
        // 简化实现，返回默认值
        // 复杂的直方图比较功能可以后续实现
        return 0.5; // 默认相似度
    }
}