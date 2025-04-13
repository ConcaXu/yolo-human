import os
import time
import cv2
import json
import sys
import threading
import socket
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError

# 添加项目根目录到系统路径
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.utils import load_config, prepare_kafka_message

class CameraSimulator:
    def __init__(self, camera_config):
        """初始化摄像头模拟器
        
        Args:
            camera_config: 摄像头配置
        """
        self.config = load_config()
        self.kafka_config = self.config["kafka"]
        self.camera_config = camera_config
        
        # 初始化摄像头
        source = self.camera_config["video_source"]
        if source.isdigit():
            source = int(source)
        self.cap = cv2.VideoCapture(source)
        self.cap.set(cv2.CAP_PROP_FRAME_WIDTH, self.camera_config["frame_width"])
        self.cap.set(cv2.CAP_PROP_FRAME_HEIGHT, self.camera_config["frame_height"])
        
        # 检查摄像头是否打开
        if not self.cap.isOpened():
            raise Exception(f"无法打开摄像头或视频文件: {source}")
        
        # 尝试初始化Kafka生产者
        self.producer = None
        self.offline_mode = False  # 离线模式标志
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_config["bootstrap_servers"],
                value_serializer=lambda v: v.encode('utf-8'),
                request_timeout_ms=20000
            )
            print(f"摄像头 {self.camera_config['id']} Kafka连接成功")
        except Exception as e:
            print(f"警告: Kafka连接失败 ({e})，摄像头 {self.camera_config['id']} 将在离线模式下运行")
            self.offline_mode = True
            
            # 创建离线存储目录
            self.offline_dir = os.path.join("data", "offline", camera_config["id"])
            os.makedirs(self.offline_dir, exist_ok=True)
            
        self.running = False
        self.thread = None
        self.frames_captured = 0
    
    def start(self):
        """启动摄像头模拟器"""
        self.running = True
        self.thread = threading.Thread(target=self._capture_and_send)
        self.thread.daemon = True
        self.thread.start()
        
        mode_str = "离线" if self.offline_mode else "在线"
        print(f"摄像头 {self.camera_config['id']} 模拟器已启动（{mode_str}模式）")
    
    def stop(self):
        """停止摄像头模拟器"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=2)  # 等待线程最多2秒
        self.cap.release()
        print(f"摄像头 {self.camera_config['id']} 模拟器已停止，共捕获 {self.frames_captured} 帧")
    
    def _capture_and_send(self):
        """捕获视频帧并发送到Kafka或保存到本地"""
        frame_interval = 1.0 / self.camera_config["fps"]
        
        while self.running:
            start_time = time.time()
            
            ret, frame = self.cap.read()
            if not ret:
                # 如果是视频文件，可以循环播放
                if not str(self.camera_config["video_source"]).isdigit():
                    self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                    continue
                else:
                    self.running = False
                    print(f"摄像头 {self.camera_config['id']} 断开连接")
                    break
            
            self.frames_captured += 1
            
            # 离线模式：直接保存图像到本地
            if self.offline_mode:
                self._save_frame_locally(frame)
            else:
                # 在线模式：发送到Kafka
                self._send_to_kafka(frame)
            
            # 控制帧率
            elapsed = time.time() - start_time
            sleep_time = max(0, frame_interval - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)
    
    def _send_to_kafka(self, frame):
        """发送帧到Kafka"""
        try:
            # 准备消息
            message = prepare_kafka_message(
                self.camera_config["id"],
                frame
            )
            
            # 发送到Kafka
            future = self.producer.send(
                self.kafka_config["video_topic"],
                message
            )
            
        except Exception as e:
            print(f'摄像头 {self.camera_config["id"]} 消息发送失败: {e}')
            # 如果发送失败，切换到离线模式
            if not self.offline_mode:
                print(f"摄像头 {self.camera_config['id']} 切换到离线模式")
                self.offline_mode = True
                self.offline_dir = os.path.join("data", "offline", self.camera_config["id"])
                os.makedirs(self.offline_dir, exist_ok=True)
            
            # 保存到本地
            self._save_frame_locally(frame)
    
    def _save_frame_locally(self, frame):
        """保存帧到本地文件"""
        if self.frames_captured % 10 == 0:  # 每10帧保存一次，避免过多文件
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            filename = f"{self.camera_config['id']}_{timestamp}_{self.frames_captured}.jpg"
            file_path = os.path.join(self.offline_dir, filename)
            
            try:
                cv2.imwrite(file_path, frame)
                if self.frames_captured % 100 == 0:  # 减少日志输出
                    print(f"摄像头 {self.camera_config['id']} 已保存帧到: {file_path}")
            except Exception as e:
                print(f"保存帧到本地失败: {e}")

class MultiCameraManager:
    def __init__(self):
        """初始化多摄像头管理器"""
        # 加载配置文件
        try:
            with open(os.path.join(os.path.dirname(__file__), "../config.json"), 'r') as f:
                self.config = json.load(f)
        except Exception as e:
            print(f"读取配置文件失败: {e}，使用默认配置")
            self.config = {
                "cameras": [
                    {
                        "id": "cam001",
                        "video_source": "0",
                        "frame_width": 640,
                        "frame_height": 480,
                        "fps": 10,
                        "location": "默认位置"
                    }
                ]
            }
            
        self.cameras = []
        self.running = False
    
    def start(self):
        """启动所有摄像头"""
        self.running = True
        print(f"正在启动 {len(self.config['cameras'])} 个摄像头...")
        
        # 创建并启动每个摄像头
        for camera_config in self.config['cameras']:
            try:
                simulator = CameraSimulator(camera_config)
                simulator.start()
                self.cameras.append(simulator)
                time.sleep(1)  # 稍微延迟，避免同时启动多个摄像头造成资源竞争
            except Exception as e:
                print(f"摄像头 {camera_config['id']} 启动失败: {e}")
        
        try:
            # 等待中断信号
            while self.running and len(self.cameras) > 0:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()
    
    def stop(self):
        """停止所有摄像头"""
        self.running = False
        for camera in self.cameras:
            camera.stop()
        print("所有摄像头已停止")

def main():
    manager = MultiCameraManager()
    manager.start()

if __name__ == "__main__":
    main() 