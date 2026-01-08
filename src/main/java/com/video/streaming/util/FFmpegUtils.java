package com.video.streaming.util;

import com.video.streaming.config.VideoStreamConfig;
import com.video.streaming.model.VideoFrame;
import lombok.extern.slf4j.Slf4j;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.OpenCVFrameConverter;
import org.bytedeco.opencv.opencv_core.Mat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.bytedeco.opencv.global.opencv_imgproc.COLOR_BGR2RGB;
import static org.bytedeco.opencv.global.opencv_imgproc.cvtColor;

/**
 * FFmpeg工具类
 * 处理视频帧编码、解码和流处理
 */
@Slf4j
public class FFmpegUtils {

    private static final String TEMP_DIR = System.getProperty("java.io.tmpdir") + "/video_segments";

    static {
        try {
            // 创建临时目录
            Files.createDirectories(Paths.get(TEMP_DIR));
        } catch (IOException e) {
            log.error("Failed to create temp directory: {}", TEMP_DIR, e);
        }
    }

    /**
     * 根据配置参数构建FFmpeg命令
     */
    public static String buildFFmpegCommand(VideoStreamConfig config, String inputStream, String outputStream) {
        StringBuilder command = new StringBuilder("ffmpeg");
        
        // 根据协议添加输入参数
        if ("rtsp".equalsIgnoreCase(config.getVideoStreamProtocol())) {
            command.append(" -rtsp_transport ").append(config.getRtspTransport());
            command.append(" -timeout ").append(config.getRtspTimeout() * 1000000); // 转换为微秒
        } else if ("gb28181".equalsIgnoreCase(config.getVideoStreamProtocol())) {
            // GB/T 28181协议的特殊参数
            command.append(" -protocol_whitelist file,http,https,tcp,tls,udp,rtp,rtsp");
        }
        
        command.append(" -i ").append(inputStream);
        
        // 视频编码参数
        if (config.getVideoCodec() != null) {
            command.append(" -c:v ").append(config.getVideoCodec());
        } else {
            command.append(" -c:v libx264"); // 默认H.264
        }
        
        if (config.getPixelFormat() != null) {
            command.append(" -pix_fmt ").append(config.getPixelFormat());
        } else {
            command.append(" -pix_fmt yuv420p"); // 默认像素格式
        }
        
        if (config.getVideoBitrate() > 0) {
            command.append(" -b:v ").append(config.getVideoBitrate()).append("k");
        }
        
        if (config.getFramerate() > 0) {
            command.append(" -r ").append(config.getFramerate());
        }
        
        command.append(" ").append(outputStream);
        command.append(" -y"); // 覆盖输出文件
        
        return command.toString();
    }

    /**
     * 从视频流中提取帧
     */
    public static Mat[] extractFramesFromStream(String streamUrl, int maxFrames, VideoStreamConfig config) {
        FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(streamUrl);
        try {
            // 根据协议设置grabber参数
            if ("rtsp".equalsIgnoreCase(config.getVideoStreamProtocol())) {
                grabber.setOption("rtsp_transport", config.getRtspTransport());
                grabber.setOption("stimeout", String.valueOf(config.getRtspTimeout() * 1000000L));
            } else if ("gb28181".equalsIgnoreCase(config.getVideoStreamProtocol())) {
                // GB/T 28181协议参数
                grabber.setOption("protocol_whitelist", "file,http,https,tcp,tls,udp,rtp,rtsp");
            }

            grabber.start();

            Mat[] frames = new Mat[maxFrames];
            int frameCount = 0;
            while (frameCount < maxFrames) {
                Frame frame = grabber.grab();
                if (frame == null) break;
                
                // 将Frame转换为Mat
                OpenCVFrameConverter.ToMat converterToMat = new OpenCVFrameConverter.ToMat();
                Mat mat = converterToMat.convert(frame);
                
                // 转换为RGB格式
                Mat rgbMat = new Mat();
                cvtColor(mat, rgbMat, COLOR_BGR2RGB);
                
                frames[frameCount] = rgbMat.clone(); // 克隆以确保数据独立
                frameCount++;
            }

            log.info("Extracted {} frames from stream: {}", frameCount, streamUrl);
            return frames;
        } catch (Exception e) {
            log.error("Error extracting frames from stream: {}", streamUrl, e);
            return null;
        } finally {
            try {
                grabber.stop();
                grabber.release();
            } catch (Exception e) {
                log.error("Error stopping/releasing grabber", e);
            }
        }
    }

    /**
     * 将帧序列编码为视频文件
     */
    public static String encodeFramesToVideo(List<VideoFrame> frames, String streamId, long startTime, VideoStreamConfig config) {
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
            String framerate = config.getFramerate() > 0 ? String.valueOf(config.getFramerate()) : "25";
            String videoCodec = config.getVideoCodec() != null ? config.getVideoCodec() : "libx264";
            String pixelFormat = config.getPixelFormat() != null ? config.getPixelFormat() : "yuv420p";
            String videoBitrate = config.getVideoBitrate() > 0 ? config.getVideoBitrate() + "k" : "2048k";

            String ffmpegCommand = String.format(
                    "ffmpeg -framerate %s -i %s/frame_%%05d.jpg -c:v %s -pix_fmt %s -b:v %s %s -y",
                    framerate, frameDir, videoCodec, pixelFormat, videoBitrate, outputPath
            );

            Process process = Runtime.getRuntime().exec(ffmpegCommand);
            int exitCode = process.waitFor();

            if (exitCode == 0) {
                log.info("Video encoded successfully: {}", outputPath);

                // 清理临时帧文件
                deleteDirectory(new File(frameDir));

                return outputPath;
            } else {
                log.error("FFmpeg encoding failed with exit code: {}", exitCode);
                logProcessOutput(process);
                return null;
            }

        } catch (Exception e) {
            log.error("Error encoding frames to video", e);
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
            log.error("Error getting file size: {}", filePath, e);
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
        try (java.io.BufferedReader reader = new java.io.BufferedReader(
                new java.io.InputStreamReader(process.getErrorStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                log.error("FFmpeg: {}", line);
            }
        } catch (IOException e) {
            log.error("Error reading process output", e);
        }
    }
}