import os
import json
import base64
import numpy as np
import cv2
from datetime import datetime
from dotenv import load_dotenv
import time

# 加载环境变量
load_dotenv()


def load_config():
    """加载配置"""
    # 尝试从config.json文件加载配置
    config_path = os.path.join(os.path.dirname(__file__), "config.json")
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # 补充从环境变量读取kafka配置（如果config中没有）
            if "kafka" not in config:
                config["kafka"] = {
                    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                    "video_topic": os.getenv("KAFKA_VIDEO_TOPIC", "video_frames"),
                    "alert_topic": os.getenv("KAFKA_ALERT_TOPIC", "human_alerts")
                }
            
            # 补充从环境变量读取YOLO配置（如果config中没有）
            if "yolo" not in config:
                config["yolo"] = {
                    "model_path": os.getenv("YOLO_MODEL_PATH", "models/yolov8n.pt"),
                    "confidence": float(os.getenv("CONFIDENCE_THRESHOLD", "0.5"))
                }
            
            # 补充从环境变量读取存储配置（如果config中没有）
            if "storage" not in config:
                config["storage"] = {
                    "save_frames": os.getenv("SAVE_DETECTED_FRAMES", "true").lower() == "true",
                    "output_dir": os.getenv("OUTPUT_DIR", "data/detected")
                }
            
            return config
        except Exception as e:
            print(f"读取配置文件失败: {e}，将使用环境变量")
    
    # 如果无法从文件加载，则从环境变量加载
    return {
        "kafka": {
            "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            "video_topic": os.getenv("KAFKA_VIDEO_TOPIC", "video_frames"),
            "alert_topic": os.getenv("KAFKA_ALERT_TOPIC", "human_alerts")
        },
        "yolo": {
            "model_path": os.getenv("YOLO_MODEL_PATH", "models/yolov8n.pt"),
            "confidence": float(os.getenv("CONFIDENCE_THRESHOLD", "0.5"))
        },
        "storage": {
            "save_frames": os.getenv("SAVE_DETECTED_FRAMES", "true").lower() == "true",
            "output_dir": os.getenv("OUTPUT_DIR", "data/detected")
        }
    }


def frame_to_bytes(frame):
    """将图像转换为字节数据"""
    _, buffer = cv2.imencode('.jpg', frame)
    return buffer.tobytes()


def bytes_to_frame(frame_bytes):
    """将字节数据转换回图像"""
    nparr = np.frombuffer(frame_bytes, np.uint8)
    return cv2.imdecode(nparr, cv2.IMREAD_COLOR)


def prepare_kafka_message(camera_id, frame, timestamp=None):
    """准备发送到Kafka的消息"""
    if timestamp is None:
        timestamp = datetime.now().isoformat()

    encoded_frame = base64.b64encode(frame_to_bytes(frame)).decode('utf-8')

    message = {
        "camera_id": camera_id,
        "timestamp": timestamp,
        "frame": encoded_frame,
        "width": frame.shape[1],
        "height": frame.shape[0]
    }

    return json.dumps(message)


def parse_kafka_message(message):
    """解析从Kafka接收的消息"""
    data = json.loads(message)
    frame_bytes = base64.b64decode(data["frame"])
    frame = bytes_to_frame(frame_bytes)

    return {
        "camera_id": data["camera_id"],
        "timestamp": data["timestamp"],
        "frame": frame,
        "width": data["width"],
        "height": data["height"]
    }


def save_detected_frame(frame, results, output_path, camera_id):
    """保存带有检测框的图像帧
    Args:
        frame: 图像帧
        results: YOLO检测结果
        output_path: 输出路径
        camera_id: 摄像头ID
    """
    for result in results:
        # 获取检测框数据并转换为numpy数组
        boxes_data = result.boxes.data.cpu().numpy()

        for data in boxes_data:
            # 获取边界框坐标和其他信息
            x1, y1, x2, y2 = data[0:4]  # 前4个值是坐标
            confidence = data[4]  # 第5个值是置信度
            class_id = int(data[5])  # 第6个值是类别ID

            # 绘制边界框
            cv2.rectangle(frame, (int(x1), int(y1)), (int(x2), int(y2)), (0, 255, 0), 2)

            # 添加类别标签和置信度
            label = f'{results[0].names[class_id]} {confidence:.2f}'
            cv2.putText(frame, label, (int(x1), int(y1 - 10)),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)

    # 构建输出文件名，包含camera_id和时间戳
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    filename = f"{camera_id}_{timestamp}.jpg"
    save_path = os.path.join(output_path, filename)
    
    # 确保输出目录存在
    os.makedirs(output_path, exist_ok=True)
    
    # 保存图像
    cv2.imwrite(save_path, frame)
