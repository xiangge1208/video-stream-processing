package com.video.streaming.util;

import com.video.streaming.model.VideoFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * FFmpeg工具类（视频编码）
 * 注意：需要系统安装FFmpeg
 */
public class FFmpegUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FFmpegUtils.class);
    private static final String TEMP_DIR = "/tmp/video-segments";

    static {
        // 创建临时目录
        try {
            Files.createDirectories(Paths.get(TEMP_DIR));
        } catch (IOException e) {
            LOG.error("Failed to create temp directory", e);
        }
    }

    /**
     * 将视频帧编码为MP4文件
     * 简化版：直接保存帧序列为图片，然后用FFmpeg合成视频
     */
    public static String encodeFramesToVideo(List<VideoFrame> frames,
                                             String streamId,
                                             long startTime) {
        if (frames == null || frames.isEmpty()) {
            return null;
        }

        try {
            // 生成唯一的输出文件名
            String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date(startTime));
            String outputFileName = String.format("%s_%s.mp4", streamId, timestamp);
            String outputPath = TEMP_DIR + File.separator + outputFileName;

            // 创建临时帧目录
            String frameDir = TEMP_DIR + File.separator + streamId + "_" + startTime;
            Files.createDirectories(Paths.get(frameDir));

            // 保存所有帧为JPEG文件
            for (int i = 0; i < frames.size(); i++) {
                String framePath = frameDir + File.separator + String.format("frame_%05d.jpg", i);
                Files.write(Paths.get(framePath), frames.get(i).getFrameData());
            }

            // 使用FFmpeg合成视频
            // 假设帧率为25fps
            String ffmpegCommand = String.format(
                    "ffmpeg -framerate 25 -i %s/frame_%%05d.jpg -c:v libx264 -pix_fmt yuv420p %s",
                    frameDir, outputPath
            );

            Process process = Runtime.getRuntime().exec(ffmpegCommand);
            int exitCode = process.waitFor();

            if (exitCode == 0) {
                LOG.info("Video encoded successfully: {}", outputPath);

                // 清理临时帧文件
                deleteDirectory(new File(frameDir));

                return outputPath;
            } else {
                LOG.error("FFmpeg encoding failed with exit code: {}", exitCode);
                logProcessOutput(process);
                return null;
            }

        } catch (Exception e) {
            LOG.error("Error encoding frames to video", e);
            return null;
        }
    }

    /**
     * 获取文件大小
     */
    public static long getFileSize(String filePath) {
        try {
            return Files.size(Paths.get(filePath));
        } catch (IOException e) {
            LOG.error("Error getting file size: {}", filePath, e);
            return 0;
        }
    }

    /**
     * 删除目录
     */
    private static void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            directory.delete();
        }
    }

    /**
     * 记录进程输出
     */
    private static void logProcessOutput(Process process) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getErrorStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                LOG.error("FFmpeg: {}", line);
            }
        } catch (IOException e) {
            LOG.error("Error reading process output", e);
        }
    }
}