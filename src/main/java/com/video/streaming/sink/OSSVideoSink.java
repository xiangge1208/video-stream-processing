package com.video.streaming.sink;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.video.streaming.config.VideoStreamConfig;
import com.video.streaming.model.VideoSegment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * OSS视频上传Sink
 */
public class OSSVideoSink extends RichSinkFunction<VideoSegment> {

    private static final Logger LOG = LoggerFactory.getLogger(OSSVideoSink.class);

    private final VideoStreamConfig config;
    private transient OSS ossClient;

    public OSSVideoSink(VideoStreamConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 初始化OSS客户端
        ossClient = new OSSClientBuilder().build(
                config.getOssEndpoint(),
                config.getOssAccessKeyId(),
                config.getOssAccessKeySecret()
        );

        LOG.info("OSS client initialized: bucket={}", config.getOssBucket());
    }

    @Override
    public void invoke(VideoSegment segment, Context context) throws Exception {
        try {
            // 生成OSS对象键
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd/HH");
            String datePrefix = sdf.format(new Date(segment.getStartTime()));

            String objectKey = String.format(
                    "videos/%s/%s/%s_%d.mp4",
                    segment.getStreamId(),
                    datePrefix,
                    segment.getStreamId(),
                    segment.getStartTime()
            );

            // 上传文件到OSS
            File file = new File(segment.getLocalFilePath());
            if (!file.exists()) {
                LOG.error("Local video file not found: {}", segment.getLocalFilePath());
                return;
            }

            ossClient.putObject(config.getOssBucket(), objectKey, file);

            LOG.info("Video segment uploaded to OSS: bucket={}, key={}, size={}bytes",
                    config.getOssBucket(), objectKey, segment.getFileSize());

            // 删除本地临时文件
            if (file.delete()) {
                LOG.debug("Local temp file deleted: {}", segment.getLocalFilePath());
            }

        } catch (Exception e) {
            LOG.error("Error uploading video segment to OSS", e);
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (ossClient != null) {
            ossClient.shutdown();
            LOG.info("OSS client closed");
        }
    }
}