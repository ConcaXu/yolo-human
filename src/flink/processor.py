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
from src.utils import load_config, parse_kafka_message, save_detected_frame


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
            self.detector = YoloDetector()

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
                if msg is None:
                    continue
                if msg.error():
                    print(f"消费者错误: {msg.error()}")
                    continue

                try:
                    # 使用utils中的parse_kafka_message函数解析消息
                    data = parse_kafka_message(msg.value().decode('utf-8'))

                    # 获取解析后的数据
                    frame = data['frame']
                    camera_id = data['camera_id']
                    timestamp = data['timestamp']

                    # 进行目标检测
                    results = self.detector.model.predict(frame)

                    # 处理检测结果
                    self.process_results(camera_id, results, frame, timestamp)

                except Exception as e:
                    print(f"处理帧时出错: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    continue

        except KeyboardInterrupt:
            print("正在停止处理器...")
        finally:
            self.consumer.close()

    def process_results(self, camera_id, results, frame, timestamp):
        """处理检测结果"""
        # 实现检测结果的处理逻辑
        # 如果需要保存检测到的帧
        if self.config["storage"]["save_frames"]:
            save_detected_frame(
                frame=frame,
                results=results,
                output_path=self.config["storage"]["output_dir"],
                camera_id=camera_id
            )


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
