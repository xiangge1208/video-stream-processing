# 视频流数据接入平台 - Flink Demo

## 项目简介

基于Apache Flink的实时视频流处理平台，实现以下功能：
- ✅ 从Kafka接收视频流数据
- ✅ 关键帧提取（场景变化检测 + 时间间隔策略）
- ✅ YOLO目标检测（基于ONNX Runtime）
- ✅ 检测结果实时写入Apache Doris
- ✅ 视频按3分钟切片保存到阿里云OSS

## 技术栈

- **流处理**: Apache Flink 1.17.2
- **消息队列**: Apache Kafka
- **视频处理**: OpenCV
- **AI推理**: ONNX Runtime + YOLOv8
- **数据存储**: Apache Doris
- **对象存储**: 阿里云OSS & MinIO
- **构建工具**: Maven

## 快速开始

### 1. 环境准备

#### 必需软件
```bash
# Java 11
java -version

# Maven 3.6+
mvn -version

# FFmpeg (视频编码)
ffmpeg -version

# Kafka (消息队列)
# Doris (数据仓库)
```

#### 下载YOLO模型
```bash
# 下载YOLOv8n ONNX模型
mkdir -p src/main/resources/models
cd src/main/resources/models

# 使用Python转换
pip install ultralytics
python yolo_export.py
```

### 2. 配置修改

编辑 `src/main/resources/application.properties`:

```properties
# Kafka地址
kafka.bootstrap.servers=localhost:9092

# Doris地址
doris.fenodes=localhost:8030
doris.username=root
doris.password=your_password

# OSS配置
oss.endpoint=http://oss-cn-hangzhou.aliyuncs.com
oss.access.key.id=YOUR_ACCESS_KEY_ID
oss.access.key.secret=YOUR_ACCESS_KEY_SECRET
oss.bucket=your-video-bucket
```

### 3. Doris建表

```sql
-- 创建数据库
CREATE DATABASE IF NOT EXISTS video_analytics;

USE video_analytics;

-- 创建检测结果表
CREATE TABLE IF NOT EXISTS video_detections (
    stream_id VARCHAR(64) COMMENT '视频流ID',
    detection_time DATETIME COMMENT '检测时间',
    frame_id BIGINT COMMENT '帧ID',
    object_class VARCHAR(32) COMMENT '目标类别',
    confidence FLOAT COMMENT '置信度',
    bbox_x1 FLOAT COMMENT '边界框左上角X',
    bbox_y1 FLOAT COMMENT '边界框左上角Y',
    bbox_x2 FLOAT COMMENT '边界框右下角X',
    bbox_y2 FLOAT COMMENT '边界框右下角Y',
    frame_url VARCHAR(512) COMMENT '关键帧URL',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'
)
DUPLICATE KEY(stream_id, detection_time)
COMMENT '视频目标检测结果表'
DISTRIBUTED BY HASH(stream_id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1",
    "storage_format" = "V2"
);

-- 创建索引
ALTER TABLE video_detections ADD INDEX idx_detection_time (detection_time) USING BITMAP;
ALTER TABLE video_detections ADD INDEX idx_object_class (object_class) USING BITMAP;
```

### 4. 编译项目

```bash
# 清理并打包
mvn clean package -DskipTests

# 生成的jar文件位于
# target/video-stream-processing-1.0-SNAPSHOT.jar
```

### 5. 本地运行

```bash
# 方式1: 在IDEA中直接运行
# 找到 VideoStreamProcessingJob.java
# 右键 -> Run 'VideoStreamProcessingJob.main()'

# 方式2: 命令行运行
mvn exec:java -Dexec.mainClass="com.video.streaming.VideoStreamProcessingJob"
```

### 6. 提交到Flink集群

```bash
# 启动Flink集群
$FLINK_HOME/bin/start-cluster.sh

# 提交任务
$FLINK_HOME/bin/flink run \
  -c com.video.streaming.VideoStreamProcessingJob \
  target/video-stream-processing-1.0-SNAPSHOT.jar

# 查看Web UI
open http://localhost:8081
```

## 测试数据生成

### Kafka生产者示例

```python
# producer.py - 模拟视频流数据发送
from kafka import KafkaProducer
import json
import base64
import cv2
import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 读取视频文件
cap = cv2.VideoCapture('test_video.mp4')
frame_id = 0

while cap.isOpened():
    ret, frame = cap.read()
    if not ret:
        break
    
    # 编码为JPEG
    _, buffer = cv2.imencode('.jpg', frame)
    frame_data = base64.b64encode(buffer).decode('utf-8')
    
    # 构造消息
    message = {
        'streamId': 'camera_001',
        'frameId': frame_id,
        'timestamp': int(time.time() * 1000),
        'frameData': frame_data,
        'frameSequence': frame_id,
        'metadata': {
            'width': frame.shape[1],
            'height': frame.shape[0],
            'fps': 25,
            'codec': 'h264'
        }
    }
    
    # 发送到Kafka
    producer.send('video-stream-topic', value=message)
    frame_id += 1
    
    # 控制帧率
    time.sleep(1/25)

cap.release()
producer.close()
```

## 项目结构

```
video-stream-processing/
├── pom.xml                                    # Maven配置
├── src/
│   ├── main/
│   │   ├── java/com/video/streaming/
│   │   │   ├── VideoStreamProcessingJob.java  # 主程序入口
│   │   │   ├── model/                         # 数据模型
│   │   │   │   ├── VideoFrame.java
│   │   │   │   ├── DetectionResult.java
│   │   │   │   └── VideoSegment.java
│   │   │   ├── function/                      # Flink函数
│   │   │   │   ├── VideoProcessFunction.java
│   │   │   │   └── VideoSegmentFunction.java
│   │   │   ├── processor/                     # 核心处理器
│   │   │   │   ├── KeyFrameExtractor.java     # 关键帧提取
│   │   │   │   ├── YOLODetector.java          # YOLO检测
│   │   │   │   └── VideoSegmentBuffer.java    # 视频缓冲
│   │   │   ├── sink/                          # Sink实现
│   │   │   │   ├── DorisSinkBuilder.java
│   │   │   │   └── OSSVideoSink.java
│   │   │   ├── serialization/                 # 序列化
│   │   │   │   └── VideoFrameDeserializationSchema.java
│   │   │   ├── config/                        # 配置管理
│   │   │   │   └── VideoStreamConfig.java
│   │   │   └── util/                          # 工具类
│   │   │       ├── ImageUtils.java
│   │   │       └── FFmpegUtils.java
│   │   └── resources/
│   │       ├── application.properties         # 应用配置
│   │       ├── log4j2.properties             # 日志配置
│   │       └── models/
│   │           └── yolov8n.onnx              # YOLO模型
│   └── test/
│       └── java/...                          # 单元测试
└── README.md
```

## 核心功能说明

### 1. 关键帧提取策略

- **时间间隔**: 每5秒强制提取一帧
- **场景变化**: 检测直方图差异超过阈值
- **运动显著性**: 检测大幅度运动（待实现）

### 2. YOLO目标检测

- 模型: YOLOv8n (轻量级版本)
- 输入: 640x640 RGB图像
- 输出: 80类COCO目标
- 置信度阈值: 0.5 (可配置)
- NMS阈值: 0.45

### 3. 视频切片保存

- 切片时长: 3分钟 (180秒)
- 编码格式: H.264
- 存储路径: `videos/{stream_id}/{date}/{timestamp}.mp4`

## 性能优化建议

### 1. 并行度配置
```java
env.setParallelism(10); // 根据CPU核数调整
```

### 2. Checkpoint配置
```java
env.enableCheckpointing(60000); // 1分钟一次
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
```

### 3. YOLO批处理
```java
// 批量处理以提升吞吐量
batchStream.map(frames -> yoloDetector.detectBatch(frames));
```

### 4. GPU加速
使用CUDA版本的ONNX Runtime以获得更好的推理性能。

## 监控指标

- **frames_processed**: 总处理帧数
- **keyframes_extracted**: 关键帧提取数
- **detections_count**: 目标检测总数
- **video_segments_created**: 视频切片数量
- **oss_upload_latency**: OSS上传延迟

## 常见问题

### Q1: OpenCV加载失败
```
解决方案: 确保OpenCV native库已正确安装
- Linux: sudo apt-get install libopencv-dev
- Mac: brew install opencv
- Windows: 下载OpenCV预编译包
```

### Q2: ONNX Runtime找不到模型
```
确保模型文件路径正确:
D:\\workspace\\video-stream-processing\\src\\main\\resources\\models\\yolov8n.onnx
```

### Q3: FFmpeg命令执行失败
```
确保FFmpeg已安装并在PATH中:
ffmpeg -version
```

### Q4: Doris写入失败
```
检查Doris配置:
1. FE节点地址是否正确
2. 表结构是否匹配
3. 用户权限是否充足
```

## 后续优化方向

- [ ] 支持多种视频编码格式
- [ ] 增加模型热更新能力
- [ ] 实现分布式推理服务
- [ ] 添加更多关键帧提取策略
- [ ] 支持实时预览和监控
- [ ] 增加异常帧检测
- [ ] 实现视频质量评估

## License

MIT License

## 联系方式

有问题欢迎提Issue!