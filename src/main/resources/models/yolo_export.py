from ultralytics import YOLO

# 加载模型
model = YOLO('yolov8n.pt')

# 导出为 ONNX，指定 opset 版本
model.export(
    format='onnx',
    opset=12,  # 使用较低的 opset 版本
    simplify=True
)