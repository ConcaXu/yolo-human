# 实时视频监控与人员检测系统

## 系统架构
摄像头 → 视频流 → Kafka → Flink+Yolo实时检测 → 告警/存储

## 组件
1. **摄像头模拟器**：使用OpenCV模拟摄像头，读取视频文件并发送到Kafka
2. **Kafka**：消息队列，存储视频帧
3. **Flink处理**：从Kafka消费消息，结合YOLOv8进行实时人员检测
4. **告警与存储**：检测到人员后进行告警并存储结果

## 安装依赖
```bash
pip install -r requirements.txt
```

## 使用方法
1. 启动Kafka服务
2. 运行摄像头模拟器：`python camera_simulator.py`
3. 运行Flink处理作业：`python flink_processor.py`
4. 运行告警组件：`python alert_service.py` 

## 
摄像头模拟器（src/camera/simulator.py）：
模拟摄像头采集视频帧
将视频帧发送到Kafka
Flink处理模块（src/flink/processor.py）：
从Kafka读取视频帧
使用YOLO进行人员检测
发送告警消息到Kafka
YOLO检测模块（src/detection/yolo_detector.py）：
使用YOLOv8模型检测图像中的人
处理检测结果并返回
告警服务（src/alert/service.py）：
从Kafka接收告警消息
处理告警信息并记录到日志
##