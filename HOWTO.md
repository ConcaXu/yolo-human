# 实时视频监控与人员检测系统使用说明

本系统模拟了视频监控与人员检测的完整流程：摄像头 → 视频流 → Kafka → Flink+Yolo实时检测 → 告警/存储

## 环境准备

1. 安装依赖：
```bash
pip install -r requirements.txt
```

2. Kafka环境准备：
   - 安装并启动Kafka服务：
     - Windows用户可使用Confluent Platform: https://www.confluent.io/download/
     - Linux/Mac用户可通过官方文档安装: https://kafka.apache.org/quickstart
   - 创建所需的主题：
```bash
# Windows
kafka-topics.bat --create --topic video_frames --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics.bat --create --topic human_alerts --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Linux/Mac
kafka-topics.sh --create --topic video_frames --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics.sh --create --topic human_alerts --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## 系统配置

系统配置在`.env`文件中，您可以根据需要修改：

- Kafka配置：服务器地址和主题名称
- 摄像头配置：摄像头ID、视频源（可以是摄像头索引或视频文件路径）
- YOLO配置：模型路径、检测置信度阈值
- 存储配置：是否保存检测到人的帧、输出目录

## 系统启动

使用以下命令启动整个系统：
```bash
python run_demo.py
```

系统会自动启动以下组件：
1. 摄像头模拟器：从摄像头或视频文件读取视频帧并发送到Kafka
2. Flink处理器：从Kafka读取视频帧，使用YOLO进行人员检测
3. 告警服务：接收检测到人员的告警信息并记录

按`Ctrl+C`可停止系统。

## 单独启动各组件

如果需要单独启动各组件，可以使用以下命令：

```bash
# 启动摄像头模拟器
python src/camera/simulator.py

# 启动Flink处理器
python src/flink/processor.py

# 启动告警服务
python src/alert/service.py
```

## 查看结果

- 检测到人员的帧会保存在`data/detected`目录
- 告警日志会保存在`data/alerts`目录

## 使用自己的视频源

修改`.env`文件中的`VIDEO_SOURCE`参数：
- 使用摄像头：设置为摄像头索引，通常为`0`（默认摄像头）
- 使用视频文件：设置为视频文件路径，例如`./data/sample.mp4`

## Zk,Kf报错总结记录
~~~text
报错日志:
D:\bigData\kafka\logs. (kafka.server.ReplicaManager)
[2025-04-10 23:26:38,704] WARN Stopping serving logs in dir D:\bigData\kafka\logs (kafka.log.LogManager)
[2025-04-10 23:26:38,706] ERROR Shutdown broker because all log dirs in D:\bigData\kafka\logs have failed (kafka.log.LogManager)

报错原因：
Kf意外退出 Topics 与 Zk不对应

解决办法：
需要删除 kf logs 以及zk中的data 和logs
~~~