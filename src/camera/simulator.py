import os
import time
import cv2
import json
import sys
import threading
from confluent_kafka import Producer

# 添加项目根目录到系统路径
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.utils import load_config, prepare_kafka_message

class CameraSimulator:
    def __init__(self):
        """初始化摄像头模拟器"""
        self.config = load_config()
        self.kafka_config = self.config["kafka"]
        self.camera_config = self.config["camera"]
        
        # 初始化摄像头
        source = self.camera_config["video_source"]
        if source.isdigit():
            source = int(source)
        self.cap = cv2.VideoCapture(source)
        self.cap.set(cv2.CAP_PROP_FRAME_WIDTH, self.camera_config["frame_width"])
        self.cap.set(cv2.CAP_PROP_FRAME_HEIGHT, self.camera_config["frame_height"])
        
        # 检查摄像头是否打开
        if not self.cap.isOpened():
            raise Exception("无法打开摄像头或视频文件")
        
        # 初始化Kafka生产者
        self.producer = Producer({
            'bootstrap.servers': self.kafka_config["bootstrap_servers"]
        })
        
        self.running = False
        self.thread = None
    
    def start(self):
        """启动摄像头模拟器"""
        self.running = True
        self.thread = threading.Thread(target=self._capture_and_send)
        self.thread.daemon = True
        self.thread.start()
        print(f"摄像头 {self.camera_config['id']} 模拟器已启动")
        
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()
    
    def stop(self):
        """停止摄像头模拟器"""
        self.running = False
        if self.thread:
            self.thread.join()
        self.cap.release()
        print(f"摄像头 {self.camera_config['id']} 模拟器已停止")
    
    def _capture_and_send(self):
        """捕获视频帧并发送到Kafka"""
        frame_interval = 1.0 / self.camera_config["fps"]
        
        while self.running:
            start_time = time.time()
            
            ret, frame = self.cap.read()
            if not ret:
                # 如果是视频文件，可以循环播放
                if not self.camera_config["video_source"].isdigit():
                    self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                    continue
                else:
                    self.running = False
                    print("摄像头断开连接")
                    break
            
            # 准备消息
            message = prepare_kafka_message(
                self.camera_config["id"],
                frame
            )
            
            # 发送到Kafka
            self.producer.produce(
                self.kafka_config["video_topic"],
                message.encode('utf-8'),
                callback=self._delivery_report
            )
            self.producer.poll(0)
            
            # 控制帧率
            elapsed = time.time() - start_time
            sleep_time = max(0, frame_interval - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)
    
    def _delivery_report(self, err, msg):
        """Kafka消息发送回调"""
        if err is not None:
            print(f'消息发送失败: {err}')
        else:
            pass  # 成功发送，不打印日志以免刷屏

def main():
    simulator = CameraSimulator()
    simulator.start()

if __name__ == "__main__":
    main() 