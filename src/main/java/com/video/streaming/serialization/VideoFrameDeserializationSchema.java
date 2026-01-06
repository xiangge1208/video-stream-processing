package com.video.streaming.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.video.streaming.model.VideoFrame;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * VideoFrame反序列化器
 */
public class VideoFrameDeserializationSchema implements DeserializationSchema<VideoFrame> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public VideoFrame deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, VideoFrame.class);
    }

    @Override
    public boolean isEndOfStream(VideoFrame nextElement) {
        return false;
    }

    @Override
    public TypeInformation<VideoFrame> getProducedType() {
        return TypeInformation.of(VideoFrame.class);
    }
}