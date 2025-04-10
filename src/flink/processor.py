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
        try:
            print("正在初始化摄像头模拟器...")
            self.config = load_config()
            self.kafka_config = self.config["kafka"]
            self.camera_config = self.config["camera"]

            # 初始化摄像头
            source = self.camera_config["video_source"]
            print(f"视频源: {source}")

            # 更安全的方式检查是否为数字
            try:
                if source.isdigit():
                    source = int(source)
                    print(f"使用摄像头索引: {source}")
            except AttributeError:
                # 如果已经是整数，会抛出AttributeError，忽略即可
                pass

            # 打开视频捕获
            print(f"正在打开视频源...")
            self.cap = cv2.VideoCapture(source)

            # 设置分辨率
            width = self.camera_config["frame_width"]
            height = self.camera_config["frame_height"]
            print(f"设置分辨率: {width}x{height}")
            self.cap.set(cv2.CAP_PROP_FRAME_WIDTH, width)
            self.cap.set(cv2.CAP_PROP_FRAME_HEIGHT, height)

            # 检查摄像头是否打开
            if not self.cap.isOpened():
                raise Exception("无法打开摄像头或视频文件")

            # 测试读取一帧
            ret, test_frame = self.cap.read()
            if not ret:
                raise Exception("无法从视频源读取帧")
            else:
                print(f"成功读取测试帧，尺寸: {test_frame.shape}")
                # 如果是视频文件，重置到开始位置
                if not isinstance(source, int):
                    self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)

            # 初始化Kafka生产者
            print(f"连接Kafka: {self.kafka_config['bootstrap_servers']}")
            self.producer = Producer({
                'bootstrap.servers': self.kafka_config["bootstrap_servers"]
            })

            self.running = False
            self.thread = None
            print("摄像头模拟器初始化完成")
        except Exception as e:
            print(f"摄像头模拟器初始化失败: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

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
            try:
                start_time = time.time()

                ret, frame = self.cap.read()
                if not ret:
                    # 如果是视频文件，可以循环播放
                    if not isinstance(self.camera_config["video_source"], int) and not str(
                            self.camera_config["video_source"]).isdigit():
                        print("视频文件结束，重新开始播放")
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
            except Exception as e:
                print(f"捕获和发送帧时出错: {str(e)}")
                import traceback
                traceback.print_exc()
                time.sleep(1)  # 出错后暂停一下再继续

    def _delivery_report(self, err, msg):
        """Kafka消息发送回调"""
        if err is not None:
            print(f'消息发送失败: {err}')
        else:
            pass  # 成功发送，不打印日志以免刷屏


def main():
    try:
        simulator = CameraSimulator()
        simulator.start()
    except Exception as e:
        print(f"运行摄像头模拟器时出错: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 