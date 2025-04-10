import os
import sys
import time
import cv2
import numpy as np
from ultralytics import YOLO

# 添加项目根目录到系统路径
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.utils import load_config

class YoloDetector:
    def __init__(self):
        """初始化YOLO检测器"""
        self.config = load_config()
        self.yolo_config = self.config["yolo"]
        
        # 载入YOLO模型
        model_path = self.yolo_config["model_path"]
        if not os.path.exists(model_path):
            print(f"模型文件不存在: {model_path}")
            print("正在下载YOLOv8模型...")
            # 使用ultralytics提供的自动下载功能
            self.model = YOLO("yolov8n.pt")
            
            # 确保models目录存在
            os.makedirs(os.path.dirname(model_path), exist_ok=True)
            
            # 保存模型到指定路径
            self.model.export(format="pt", save=True)
            print(f"模型已保存到: {model_path}")
        else:
            self.model = YOLO(model_path)
        
        self.confidence = self.yolo_config["confidence"]
        print(f"YOLO检测器已初始化，置信度阈值: {self.confidence}")
    
    def detect_humans(self, frame):
        """检测图像中的人"""
        results = self.model(frame, verbose=False)
        
        # 提取人的检测结果（类别为0）
        humans = []
        for result in results:
            boxes = result.boxes
            for box in boxes:
                cls = int(box.cls.item())
                conf = box.conf.item()
                
                # 类别0是'person'，检查置信度是否超过阈值
                if cls == 0 and conf >= self.confidence:
                    # 获取边界框坐标
                    x1, y1, x2, y2 = box.xyxy[0].tolist()
                    humans.append({
                        'box': [x1, y1, x2, y2],
                        'confidence': conf
                    })
        
        # 返回检测结果
        detection_result = {
            'detected': len(humans) > 0,
            'count': len(humans),
            'humans': humans,
            'boxes': [human['box'] for human in humans]
        }
        
        return detection_result

    def visualize_detection(self, frame, detection_result):
        """在图像上可视化检测结果"""
        # 绘制边界框
        for human in detection_result['humans']:
            box = human['box']
            conf = human['confidence']
            x1, y1, x2, y2 = map(int, box)
            
            # 绘制边界框和置信度
            cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 255, 0), 2)
            cv2.putText(
                frame, 
                f"Person: {conf:.2f}", 
                (x1, y1 - 10), 
                cv2.FONT_HERSHEY_SIMPLEX, 
                0.5, 
                (0, 255, 0), 
                2
            )
        
        # 绘制人数统计
        cv2.putText(
            frame, 
            f"Humans: {detection_result['count']}", 
            (10, 30), 
            cv2.FONT_HERSHEY_SIMPLEX, 
            1, 
            (0, 0, 255), 
            2
        )
        
        return frame 