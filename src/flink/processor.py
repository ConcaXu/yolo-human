import os
import time
import cv2
import json
from confluent_kafka import Consumer
import numpy as np
import sys

from src.detection.yolo_detector import YoloDetector

# 添加项目根目录到系统路径
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.utils import load_config


class FlinkProcessor:
    def __init__(self):
        """初始化Flink处理器"""
        try:
            print("正在初始化Flink处理器...")
            self.config = load_config()
            self.kafka_config = self.config["kafka"]

            # 初始化Kafka消费者
            self.consumer = Consumer({
                'bootstrap.servers': self.kafka_config["bootstrap_servers"],
                'group.id': 'flink_processor_group',
                'auto.offset.reset': 'latest'
            })

            # 订阅Kafka主题
            self.consumer.subscribe([self.kafka_config["video_topic"]])

            # 初始化YOLO检测器
            self.detector = YoloDetector()  # 您需要实现这个类

            print("Flink处理器初始化完成")

        except Exception as e:
            print(f"Flink处理器初始化失败: {str(e)}")
            raise

    def start(self):
        """启动处理器"""
        print("开始处理视频流...")
        try:
            while True:
                msg = self.consumer.poll(1.0)
                print("msg---->", msg)
                if msg is None:
                    continue
                if msg.error():
                    print(f"消费者错误: {msg.error()}")
                    continue

                # 解析消息
                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    frame_data = np.frombuffer(bytes.fromhex(data['frame']), dtype=np.uint8)
                    frame = cv2.imdecode(frame_data, cv2.IMREAD_COLOR)

                    # 进行目标检测
                    results = self.detector.predict(frame)

                    # 处理检测结果
                    self.process_results(data['camera_id'], results)

                except Exception as e:
                    print(f"处理帧时出错: {str(e)}")
                    continue

        except KeyboardInterrupt:
            print("正在停止处理器...")
        finally:
            self.consumer.close()

    def process_results(self, camera_id, results):
        """处理检测结果"""
        # 实现检测结果的处理逻辑
        # 例如：保存到数据库、触发告警等
        pass


def main():
    try:
        processor = FlinkProcessor()
        processor.start()
    except Exception as e:
        print(f"运行Flink处理器时出错: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
