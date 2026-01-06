package com.video.streaming.function;

import com.video.streaming.model.VideoSegment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 视频段处理函数（可选的额外处理逻辑）
 */
public class VideoSegmentFunction
        extends KeyedProcessFunction<String, VideoSegment, VideoSegment> {

    @Override
    public void processElement(VideoSegment segment,
                               Context ctx,
                               Collector<VideoSegment> out) throws Exception {
        // 这里可以添加额外的处理逻辑
        // 例如：视频质量检查、元数据提取等

        // 直接传递到下游
        out.collect(segment);
    }
}