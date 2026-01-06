package com.video.streaming.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.video.streaming.model.VideoFrame;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.imgcodecs.Imgcodecs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 视频流模拟数据生产者
 * 功能：
 * 1. 读取本地图片文件夹
 * 2. 模拟视频帧发送到Kafka
 * 3. 支持多流并发发送
 * 4. 可配置帧率和持续时间
 */
public class VideoStreamMockProducer {

    private static final Logger LOG = LoggerFactory.getLogger(VideoStreamMockProducer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // 配置参数
    private final String bootstrapServers;
    private final String topic;
    private final String imageDirectory;
    private final int fps;
    private final int durationSeconds;
    private final List<String> streamIds;

    private KafkaProducer<String, String> producer;
    private final AtomicLong globalFrameId = new AtomicLong(0);

    static {
        // 加载OpenCV库
        nu.pattern.OpenCV.loadLocally();
    }

    public VideoStreamMockProducer(String bootstrapServers,
                                   String topic,
                                   String imageDirectory,
                                   int fps,
                                   int durationSeconds,
                                   List<String> streamIds) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.imageDirectory = imageDirectory;
        this.fps = fps;
        this.durationSeconds = durationSeconds;
        this.streamIds = streamIds;
        initProducer();
    }

    /**
     * 初始化Kafka生产者
     */
    private void initProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        producer = new KafkaProducer<>(props);
        LOG.info("Kafka producer initialized: {}", bootstrapServers);
    }

    /**
     * 开始发送数据
     */
    public void start() throws Exception {
        LOG.info("Starting video stream mock producer...");
        LOG.info("Image directory: {}", imageDirectory);
        LOG.info("FPS: {}, Duration: {}s, Streams: {}", fps, durationSeconds, streamIds);

        // 加载所有图片
        List<String> imagePaths = loadImagePaths(imageDirectory);
        if (imagePaths.isEmpty()) {
            throw new IllegalStateException("No images found in directory: " + imageDirectory);
        }
        LOG.info("Loaded {} images", imagePaths.size());

        // 为每个流创建线程
        List<Thread> threads = new ArrayList<>();
        for (String streamId : streamIds) {
            Thread thread = new Thread(() -> sendStreamData(streamId, imagePaths));
            thread.setName("Stream-" + streamId);
            threads.add(thread);
            thread.start();
        }

        // 等待所有线程完成
        for (Thread thread : threads) {
            thread.join();
        }

        LOG.info("All streams completed");
        close();
    }

    /**
     * 发送单个流的数据
     */
    private void sendStreamData(String streamId, List<String> imagePaths) {
        LOG.info("Stream {} started", streamId);

        long startTime = System.currentTimeMillis();
        long frameInterval = 1000 / fps; // 毫秒
        int totalFrames = fps * durationSeconds;
        int imageIndex = 0;
        int frameSequence = 0;

        long sentFrames = 0;
        long successCount = 0;
        long errorCount = 0;

        try {
            for (int i = 0; i < totalFrames; i++) {
                long frameStartTime = System.currentTimeMillis();

                // 循环使用图片
                String imagePath = imagePaths.get(imageIndex % imagePaths.size());
                imageIndex++;

                // 构建VideoFrame
                VideoFrame frame = buildVideoFrame(streamId, imagePath, frameSequence++);
                if (frame == null) {
                    errorCount++;
                    continue;
                }

                // 发送到Kafka
                try {
                    String json = objectMapper.writeValueAsString(frame);
                    ProducerRecord<String, String> record = new ProducerRecord<>(
                            topic,
                            streamId,
                            json
                    );

                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            LOG.error("Error sending frame: {}", exception.getMessage());
                        }
                    });

                    sentFrames++;
                    successCount++;

                    // 定期打印进度
                    if (sentFrames % 100 == 0) {
                        LOG.info("Stream {}: Sent {} frames ({} success, {} errors)",
                                streamId, sentFrames, successCount, errorCount);
                    }

                } catch (Exception e) {
                    LOG.error("Error sending frame for stream {}", streamId, e);
                    errorCount++;
                }

                // 控制帧率
                long elapsed = System.currentTimeMillis() - frameStartTime;
                long sleepTime = frameInterval - elapsed;
                if (sleepTime > 0) {
                    Thread.sleep(sleepTime);
                }
            }

            long totalTime = System.currentTimeMillis() - startTime;
            LOG.info("Stream {} completed: {} frames in {}ms (avg {}fps)",
                    streamId, sentFrames, totalTime, (sentFrames * 1000.0 / totalTime));
            LOG.info("Stream {} statistics: Success={}, Errors={}",
                    streamId, successCount, errorCount);

        } catch (Exception e) {
            LOG.error("Error in stream {}", streamId, e);
        }
    }

    /**
     * 构建VideoFrame对象
     */
    private VideoFrame buildVideoFrame(String streamId, String imagePath, int frameSequence) {
        try {
            // 读取图片
            Mat image = Imgcodecs.imread(imagePath);
            if (image.empty()) {
                LOG.warn("Failed to read image: {}", imagePath);
                return null;
            }

            // 编码为JPEG
            MatOfByte buffer = new MatOfByte();
            Imgcodecs.imencode(".jpg", image, buffer);
            byte[] frameData = buffer.toArray();

            // 释放资源
            image.release();
            buffer.release();

            // 构建VideoFrame
            VideoFrame frame = VideoFrame.builder()
                    .streamId(streamId)
                    .frameId(globalFrameId.incrementAndGet())
                    .timestamp(System.currentTimeMillis())
                    .frameData(frameData)
                    .frameSequence(frameSequence)
                    .metadata(VideoFrame.FrameMetadata.builder()
                            .width(1920)
                            .height(1080)
                            .fps(fps)
                            .codec("jpeg")
                            .build())
                    .build();

            return frame;

        } catch (Exception e) {
            LOG.error("Error building video frame from image: {}", imagePath, e);
            return null;
        }
    }

    /**
     * 加载图片文件路径
     */
    private List<String> loadImagePaths(String directory) throws IOException {
        try (Stream<Path> paths = Files.walk(Paths.get(directory))) {
            return paths
                    .filter(Files::isRegularFile)
                    .filter(path -> {
                        String fileName = path.getFileName().toString().toLowerCase();
                        return fileName.endsWith(".jpg") ||
                                fileName.endsWith(".jpeg") ||
                                fileName.endsWith(".png");
                    })
                    .map(Path::toString)
                    .sorted()
                    .collect(Collectors.toList());
        }
    }

    /**
     * 关闭生产者
     */
    public void close() {
        if (producer != null) {
            producer.flush();
            producer.close();
            LOG.info("Kafka producer closed");
        }
    }

    /**
     * 主函数
     */
    public static void main(String[] args) {
        try {
            // 配置参数（可以从命令行或配置文件读取）
            String bootstrapServers = "localhost:9092";
            String topic = "video-stream-topic";
            String imageDirectory = "src/test/resources/test-images"; // 测试图片目录
            int fps = 25; // 帧率
            int durationSeconds = 60; // 持续时间（秒）
            List<String> streamIds = Arrays.asList("camera_001", "camera_002", "camera_003");

            // 从命令行参数读取配置
            if (args.length >= 1) {
                bootstrapServers = args[0];
            }
            if (args.length >= 2) {
                topic = args[1];
            }
            if (args.length >= 3) {
                imageDirectory = args[2];
            }
            if (args.length >= 4) {
                fps = Integer.parseInt(args[3]);
            }
            if (args.length >= 5) {
                durationSeconds = Integer.parseInt(args[4]);
            }

            // 创建并启动生产者
            VideoStreamMockProducer producer = new VideoStreamMockProducer(
                    bootstrapServers,
                    topic,
                    imageDirectory,
                    fps,
                    durationSeconds,
                    streamIds
            );

            producer.start();

        } catch (Exception e) {
            LOG.error("Error running mock producer", e);
            System.exit(1);
        }
    }
}

/**
 * 简化版本 - 发送单张图片循环
 */
class SimpleImageProducer {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleImageProducer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        nu.pattern.OpenCV.loadLocally();
    }

    public static void main(String[] args) throws Exception {

        String bootstrapServers = "localhost:9092";
        String topic = "video-stream-topic";
        String imagePath = "src/test/resources/test-images/sample.jpg";
        String streamId = "camera_001";
        int totalFrames = 100;

        // 创建Kafka生产者
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 读取图片
        Mat image = Imgcodecs.imread(imagePath);
        if (image.empty()) {
            LOG.error("Failed to read image: {}", imagePath);
            System.exit(1);
        }

        MatOfByte buffer = new MatOfByte();
        Imgcodecs.imencode(".jpg", image, buffer);
        byte[] frameData = buffer.toArray();

        LOG.info("Image loaded: {}x{}, size: {} bytes",
                image.cols(), image.rows(), frameData.length);

        // 发送帧
        for (int i = 0; i < totalFrames; i++) {
            VideoFrame frame = VideoFrame.builder()
                    .streamId(streamId)
                    .frameId((long) i)
                    .timestamp(System.currentTimeMillis())
                    .frameData(frameData)
                    .frameSequence(i)
                    .metadata(VideoFrame.FrameMetadata.builder()
                            .width(image.cols())
                            .height(image.rows())
                            .fps(25)
                            .codec("jpeg")
                            .build())
                    .build();

            String json = objectMapper.writeValueAsString(frame);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, streamId, json);

            producer.send(record);

            if ((i + 1) % 10 == 0) {
                LOG.info("Sent {} frames", i + 1);
            }

            Thread.sleep(40); // 25fps
        }

        // 清理资源
        image.release();
        buffer.release();
        producer.flush();
        producer.close();

        LOG.info("Completed sending {} frames", totalFrames);
    }
}