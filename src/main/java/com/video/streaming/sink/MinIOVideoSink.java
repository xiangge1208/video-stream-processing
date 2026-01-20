package com.video.streaming.sink;

import com.video.streaming.config.VideoStreamConfig;
import com.video.streaming.model.VideoSegment;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * MinIO视频上传Sink
 */
public class MinIOVideoSink extends RichSinkFunction<VideoSegment> {

    private static final Logger LOG = LoggerFactory.getLogger(MinIOVideoSink.class);

    private final VideoStreamConfig config;
    private transient MinioClient minioClient;

    public MinIOVideoSink(VideoStreamConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 初始化MinIO客户端
        minioClient = MinioClient.builder()
                .endpoint(config.getMinioEndpoint())
                .credentials(config.getMinioAccessKeyId(), config.getMinioAccessKeySecret())
                .build();

        LOG.info("MinIO client initialized: bucket={}, secure={}",
                config.getMinioBucket(), config.isMinioSecure());
    }

    @Override
    public void invoke(VideoSegment segment, Context context) throws Exception {
        FileInputStream fileInputStream = null;
        try {
            // 生成MinIO对象键
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd/HH");
            String datePrefix = sdf.format(new Date(segment.getStartTime()));

            String objectKey = String.format(
                    "videos/%s/%s/%s_%d.mp4",
                    segment.getStreamId(),
                    datePrefix,
                    segment.getStreamId(),
                    segment.getStartTime()
            );

            // 上传文件到MinIO
            File file = new File(segment.getLocalFilePath());
            if (!file.exists()) {
                LOG.error("Local video file not found: {}", segment.getLocalFilePath());
                return;
            }

            fileInputStream = new FileInputStream(file);

            PutObjectArgs putObjectArgs = PutObjectArgs.builder()
                    .bucket(config.getMinioBucket())
                    .object(objectKey)
                    .stream(fileInputStream, file.length(), -1)
                    .build();

            minioClient.putObject(putObjectArgs);

            LOG.info("Video segment uploaded to MinIO: bucket={}, key={}, size={}bytes",
                    config.getMinioBucket(), objectKey, segment.getFileSize());

            // 删除本地临时文件
            if (file.delete()) {
                LOG.debug("Local temp file deleted: {}", segment.getLocalFilePath());
            }

        } catch (Exception e) {
            LOG.error("Error uploading video segment to MinIO", e);
            throw e;
        } finally {
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (Exception e) {
                    LOG.warn("Error closing file input stream", e);
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        // MinIO客户端不需要显式关闭
        LOG.info("MinIO sink closed");
    }
}
