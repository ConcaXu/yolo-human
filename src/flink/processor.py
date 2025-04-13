import os
import time
import cv2
import json
import threading
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError
import numpy as np
import sys
import queue
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from src.detection.yolo_detector import YoloDetector

# 添加项目根目录到系统路径
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.utils import load_config, parse_kafka_message, save_detected_frame


class FlinkProcessor:
    def __init__(self):
        """初始化Flink处理器"""
        try:
            print("正在初始化Flink处理器...")
            
            # 加载配置文件
            try:
                with open(os.path.join(os.path.dirname(__file__), "../config.json"), 'r') as f:
                    self.config = json.load(f)
            except Exception as e:
                print(f"读取配置文件失败: {e}，使用默认配置")
                self.config = {
                    "kafka": {
                        "bootstrap_servers": "localhost:9092",
                        "video_topic": "video_frames",
                        "alert_topic": "human_alerts"
                    },
                    "processor": {"num_threads": 2},
                    "yolo": {
                        "model_path": "models/yolov8n.pt",
                        "confidence": 0.5
                    },
                    "storage": {
                        "save_frames": True,
                        "output_dir": "data/detected"
                    },
                    "cameras": [
                        {
                            "id": "cam001",
                            "location": "默认位置"
                        }
                    ]
                }
                
            self.kafka_config = self.config["kafka"]
            self.processor_config = self.config.get("processor", {"num_threads": 2})
            self.storage_config = self.config.get("storage", {"save_frames": True, "output_dir": "data/detected"})
            
            # 摄像头列表
            self.cameras = {cam["id"]: cam for cam in self.config.get("cameras", [])}
            
            # 初始化线程池
            self.num_threads = self.processor_config["num_threads"]
            self.executor = ThreadPoolExecutor(max_workers=self.num_threads)
            
            # 初始化任务队列
            self.task_queue = queue.Queue(maxsize=100)
            
            # 初始化Kafka消费者和生产者
            self.consumer = None
            self.producer = None
            self.offline_mode = False
            
            try:
                # 初始化Kafka消费者
                self.consumer = KafkaConsumer(
                    bootstrap_servers=self.kafka_config["bootstrap_servers"],
                    group_id='flink_processor_group',
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    # 修复超时设置
                    request_timeout_ms=20000,
                    session_timeout_ms=10000,
                    consumer_timeout_ms=5000
                )
                
                # 订阅主题
                self.consumer.subscribe([self.kafka_config["video_topic"]])
                
                # 测试连接
                self.consumer.topics()
                
                # 初始化Kafka生产者（用于发送告警）
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_config["bootstrap_servers"],
                    value_serializer=lambda v: v.encode('utf-8'),
                    # 修复超时设置
                    request_timeout_ms=20000
                )
                
                print("Kafka连接成功")
            except Exception as e:
                print(f"警告: Kafka连接失败 ({e})，处理器将在离线模式下运行")
                self.offline_mode = True
                if self.consumer:
                    try:
                        self.consumer.close(autocommit=False)
                    except:
                        pass
                    self.consumer = None
                if self.producer:
                    try:
                        self.producer.close()
                    except:
                        pass
                    self.producer = None

            # 初始化YOLO检测器
            self.detector = YoloDetector()
            
            # 运行状态标志
            self.running = False
            
            # 处理的帧统计
            self.processed_frames = 0
            self.last_stats_time = time.time()
            
            # 离线目录
            self.offline_dirs = [os.path.join("data", "offline", cam_id) for cam_id in self.cameras.keys()]
            
            os.makedirs(self.storage_config["output_dir"], exist_ok=True)
            
            print(f"Flink处理器初始化完成，工作线程数: {self.num_threads}" + (" (离线模式)" if self.offline_mode else ""))

        except Exception as e:
            print(f"Flink处理器初始化失败: {str(e)}")
            import traceback
            traceback.print_exc()
            
            # 确保基本变量存在
            self.offline_mode = True
            self.running = False
            self.num_threads = 2
            
            if not hasattr(self, 'task_queue'):
                self.task_queue = queue.Queue(maxsize=100)
                
            if not hasattr(self, 'processed_frames'):
                self.processed_frames = 0
                self.last_stats_time = time.time()
                
            if not hasattr(self, 'storage_config'):
                self.storage_config = {"save_frames": True, "output_dir": "data/detected"}
                os.makedirs(self.storage_config["output_dir"], exist_ok=True)
            
            if not hasattr(self, 'cameras'):
                self.cameras = {"cam001": {"id": "cam001", "location": "默认位置"}}
                
            if not hasattr(self, 'offline_dirs'):
                self.offline_dirs = [os.path.join("data", "offline", cam_id) for cam_id in self.cameras.keys()]
            
            # 尝试初始化YOLO检测器
            try:
                if not hasattr(self, 'detector'):
                    self.detector = YoloDetector()
            except:
                print("无法初始化YOLO检测器")
                raise

    def start(self):
        """启动处理器"""
        self.running = True
        
        # 启动离线检测线程
        self.offline_thread = threading.Thread(target=self._process_offline_files)
        self.offline_thread.daemon = True
        self.offline_thread.start()
        
        # 如果在线模式，启动Kafka消费线程
        if not self.offline_mode and self.consumer:
            # 启动消费者线程
            self.consumer_thread = threading.Thread(target=self._consume_messages)
            self.consumer_thread.daemon = True
            self.consumer_thread.start()
        
        # 启动处理线程
        self.worker_threads = []
        for i in range(self.num_threads):
            worker = threading.Thread(target=self._process_frames, args=(i,))
            worker.daemon = True
            worker.start()
            self.worker_threads.append(worker)
        
        # 启动状态监控线程
        self.stats_thread = threading.Thread(target=self._print_stats)
        self.stats_thread.daemon = True
        self.stats_thread.start()
        
        mode_str = "离线" if self.offline_mode else "在线"
        print(f"开始处理视频流，使用 {self.num_threads} 个工作线程（{mode_str}模式）...")
        
        try:
            # 主线程等待
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print("正在停止处理器...")
            self.stop()
        
    def stop(self):
        """停止处理器"""
        self.running = False
        
        # 等待所有工作线程完成
        for thread in self.worker_threads:
            thread.join(timeout=2)
            
        if hasattr(self, 'offline_thread'):
            self.offline_thread.join(timeout=2)
            
        if hasattr(self, 'consumer_thread'):
            self.consumer_thread.join(timeout=2)
            
        if self.consumer:
            try:
                self.consumer.close()
            except:
                pass
            
        if self.producer:
            try:
                self.producer.close()
            except:
                pass
            
        print("处理器已停止")

    def _consume_messages(self):
        """从Kafka消费消息并添加到任务队列"""
        while self.running and not self.offline_mode and self.consumer:
            try:
                # 批量获取消息
                message_batch = self.consumer.poll(timeout_ms=1000, max_records=10)
                
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        try:
                            # 将消息放入队列
                            if message.value:
                                self.task_queue.put({"type": "kafka", "data": message.value.decode('utf-8')}, block=True, timeout=1)
                        except queue.Full:
                            print("任务队列已满，丢弃消息")
                        except Exception as e:
                            print(f"处理消息时出错: {str(e)}")
            except Exception as e:
                print(f"消费者错误: {e}")
                time.sleep(1)  # 出错后稍等一下再继续
                
                # 如果出现Kafka错误，切换到离线模式
                if isinstance(e, (NoBrokersAvailable, KafkaError)):
                    print(f"Kafka连接丢失，切换到离线模式")
                    self.offline_mode = True
                    try:
                        self.consumer.close()
                    except:
                        pass
                    break
    
    def _process_offline_files(self):
        """处理离线文件目录"""
        while self.running:
            try:
                # 检查每个摄像头的离线目录
                for cam_id in self.cameras.keys():
                    offline_dir = os.path.join("data", "offline", cam_id)
                    
                    # 确保目录存在
                    if not os.path.exists(offline_dir):
                        continue
                    
                    # 获取目录中的jpg文件
                    try:
                        files = [f for f in os.listdir(offline_dir) if f.endswith('.jpg')]
                    except:
                        continue
                    
                    for file in files:
                        try:
                            # 检查是否是已处理的文件
                            if os.path.exists(os.path.join(offline_dir, "processed", file)):
                                continue
                                
                            # 读取图像文件
                            file_path = os.path.join(offline_dir, file)
                            frame = cv2.imread(file_path)
                            
                            if frame is None:
                                continue
                                
                            # 从文件名获取时间戳
                            timestamp = datetime.now().isoformat()
                            try:
                                timestamp_str = file.split('_')[1]
                                timestamp = f"{timestamp_str[:4]}-{timestamp_str[4:6]}-{timestamp_str[6:8]}T{timestamp_str[9:11]}:{timestamp_str[11:13]}:{timestamp_str[13:15]}"
                            except:
                                pass
                                
                            # 放入队列处理
                            task = {
                                "type": "offline",
                                "camera_id": cam_id,
                                "timestamp": timestamp,
                                "frame": frame,
                                "file_path": file_path
                            }
                            
                            try:
                                self.task_queue.put(task, block=False)
                            except queue.Full:
                                # 队列满，跳过当前文件
                                break
                                
                        except Exception as e:
                            print(f"处理离线文件 {file} 时出错: {e}")
                            
                # 休眠一段时间再检查
                time.sleep(5)
                
            except Exception as e:
                print(f"离线文件处理出错: {e}")
                time.sleep(10)
    
    def _process_frames(self, worker_id):
        """工作线程函数，处理队列中的帧"""
        print(f"工作线程 {worker_id} 已启动")
        
        while self.running:
            try:
                # 从队列获取任务
                task = self.task_queue.get(block=True, timeout=1)
                
                # 处理不同类型的任务
                if task["type"] == "kafka":
                    # 解析Kafka消息
                    data = parse_kafka_message(task["data"])
                    
                    # 获取解析后的数据
                    frame = data['frame']
                    camera_id = data['camera_id']
                    timestamp = data['timestamp']
                    
                    # 进行目标检测
                    self._detect_and_process(frame, camera_id, timestamp)
                    
                elif task["type"] == "offline":
                    # 处理离线文件
                    frame = task["frame"]
                    camera_id = task["camera_id"]
                    timestamp = task["timestamp"]
                    file_path = task["file_path"]
                    
                    # 进行目标检测
                    detection_info = self._detect_and_process(frame, camera_id, timestamp)
                    
                    # 如果检测到人，移动文件到已处理目录
                    if detection_info and detection_info.get('count', 0) > 0:
                        try:
                            offline_dir = os.path.dirname(file_path)
                            processed_dir = os.path.join(offline_dir, "processed")
                            os.makedirs(processed_dir, exist_ok=True)
                            
                            filename = os.path.basename(file_path)
                            new_path = os.path.join(processed_dir, filename)
                            
                            os.rename(file_path, new_path)
                        except Exception as e:
                            print(f"移动离线文件失败: {e}")
                
                # 任务完成
                self.task_queue.task_done()
                
                # 统计已处理帧数
                self.processed_frames += 1
                
            except queue.Empty:
                # 队列为空，继续等待
                continue
            except Exception as e:
                print(f"工作线程 {worker_id} 处理帧时出错: {str(e)}")
                import traceback
                traceback.print_exc()
    
    def _detect_and_process(self, frame, camera_id, timestamp):
        """检测并处理单帧"""
        try:
            # 进行目标检测
            results = self.detector.model.predict(frame)
            
            # 解析检测结果
            detection_info = self._parse_detection_results(results)
            
            # 如果检测到人，发送告警
            if detection_info['count'] > 0:
                self._send_alert(camera_id, detection_info, timestamp)
            
            # 如果需要保存检测到的帧
            if self.storage_config["save_frames"] and detection_info['count'] > 0:
                save_detected_frame(
                    frame=frame,
                    results=results,
                    output_path=self.storage_config["output_dir"],
                    camera_id=camera_id
                )
                
            return detection_info
            
        except Exception as e:
            print(f"检测处理帧时出错: {e}")
            return None
    
    def _print_stats(self):
        """打印处理统计信息"""
        while self.running:
            time.sleep(10)  # 每10秒打印一次统计信息
            
            current_time = time.time()
            elapsed = current_time - self.last_stats_time
            
            if elapsed > 0:
                fps = self.processed_frames / elapsed
                print(f"处理速度: {fps:.2f} 帧/秒，队列长度: {self.task_queue.qsize()}，运行模式: {'离线' if self.offline_mode else '在线'}")
                
                # 重置计数器
                self.processed_frames = 0
                self.last_stats_time = current_time

    def _parse_detection_results(self, results):
        """解析YOLO检测结果，提取人的信息"""
        humans = []
        for result in results:
            boxes = result.boxes
            for box in boxes:
                cls = int(box.cls.item())
                conf = box.conf.item()
                
                # 类别0是'person'
                if cls == 0 and conf >= self.config["yolo"]["confidence"]:
                    # 获取边界框坐标
                    x1, y1, x2, y2 = box.xyxy[0].tolist()
                    humans.append({
                        'box': [x1, y1, x2, y2],
                        'confidence': conf
                    })
        
        return {
            'detected': len(humans) > 0,
            'count': len(humans),
            'humans': humans
        }
    
    def _send_alert(self, camera_id, detection_info, timestamp):
        """发送告警消息到Kafka或记录到本地文件"""
        alert = {
            'camera_id': camera_id,
            'timestamp': timestamp,
            'detection': detection_info
        }
        
        # 如果在线模式，发送到Kafka
        if not self.offline_mode and self.producer:
            try:
                self.producer.send(
                    self.kafka_config["alert_topic"],
                    json.dumps(alert)
                )
                return
            except Exception as e:
                print(f"发送告警消息失败: {e}")
                # 如果发送失败，转为离线模式记录
                
        # 离线模式或发送失败，写入本地文件
        try:
            alert_dir = os.path.join("data", "alerts")
            os.makedirs(alert_dir, exist_ok=True)
            
            # 创建每个摄像头的告警子目录
            cam_alert_dir = os.path.join(alert_dir, camera_id)
            os.makedirs(cam_alert_dir, exist_ok=True)
            
            # 计算时间戳字符串（用于文件名）
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # 写入告警文件
            alert_file = os.path.join(cam_alert_dir, f"alert_{timestamp_str}.json")
            with open(alert_file, 'w', encoding='utf-8') as f:
                json.dump(alert, f, ensure_ascii=False, indent=2)
                
            print(f"离线告警已保存到: {alert_file}")
            
        except Exception as e:
            print(f"保存离线告警失败: {e}")


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
