import onnx

# 加载模型
model = onnx.load("yolov8n.onnx")

# 打印输入信息
print("=== Inputs ===")
for input in model.graph.input:
    print(f"Name: {input.name}")
    print(f"Shape: {[dim.dim_value for dim in input.type.tensor_type.shape.dim]}")
    print(f"Type: {input.type.tensor_type.elem_type}")
    print()

# 打印输出信息
print("=== Outputs ===")
for output in model.graph.output:
    print(f"Name: {output.name}")
    print(f"Shape: {[dim.dim_value for dim in output.type.tensor_type.shape.dim]}")
    print(f"Type: {output.type.tensor_type.elem_type}")
    print()