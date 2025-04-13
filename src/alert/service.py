import os
import sys
import time
import json
import threading
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError
from datetime import datetime

# 添加项目根目录到系统路径
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.utils import load_config

class AlertService:
    def __init__(self):
        """初始化告警服务"""
        try:
            # 加载配置文件
            try:
                with open(os.path.join(os.path.dirname(__file__), "../config.json"), 'r') as f:
                    self.config = json.load(f)
            except Exception as e:
                print(f"读取配置文件失败: {e}，使用默认配置")
                self.config = {
                    "kafka": {
                        "bootstrap_servers": "localhost:9092",
                        "alert_topic": "human_alerts"
                    },
                    "cameras": [
                        {
                            "id": "cam001",
                            "location": "默认位置"
                        }
                    ]
                }
                
            self.kafka_config = self.config["kafka"]
            
            # 摄像头列表
            self.cameras = {cam["id"]: cam for cam in self.config["cameras"]}
            print(f"已配置 {len(self.cameras)} 个摄像头的告警监控")
            
            # 初始化Kafka消费者
            self.consumer = None
            self.offline_mode = False
            
            try:
                # 增加超时设置，避免长时间阻塞
                self.consumer = KafkaConsumer(
                    bootstrap_servers=self.kafka_config["bootstrap_servers"],
                    group_id='alert-service',
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    # 设置正确的超时值
                    request_timeout_ms=20000,
                    session_timeout_ms=10000,
                    consumer_timeout_ms=5000,
                    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                )
                
                # 订阅主题
                self.consumer.subscribe([self.kafka_config["alert_topic"]])
                
                # 测试连接
                self.consumer.topics()
                
                print(f"告警服务 Kafka连接成功")
            except Exception as e:
                print(f"警告: Kafka连接失败 ({e})，告警服务将在离线模式下运行")
                self.offline_mode = True
                if self.consumer:
                    try:
                        self.consumer.close(autocommit=False)
                    except:
                        pass
                    self.consumer = None
            
            self.running = False
            self.thread = None
            
            # 告警统计
            self.alert_counts = {cam_id: 0 for cam_id in self.cameras.keys()}
            self.start_time = datetime.now()
            
            # 告警日志文件
            self.log_dir = "data/alerts"
            os.makedirs(self.log_dir, exist_ok=True)
            self.log_file = os.path.join(self.log_dir, f"alerts_{datetime.now().strftime('%Y%m%d')}.log")
            
            # 离线目录监控
            self.offline_dirs = [os.path.join("data", "offline", cam_id) for cam_id in self.cameras.keys()]
            
            print("告警服务已初始化" + (" (离线模式)" if self.offline_mode else ""))
            
        except Exception as e:
            print(f"告警服务初始化失败: {e}")
            import traceback
            traceback.print_exc()
            # 强制启用离线模式
            self.offline_mode = True
            self.consumer = None
            
            # 确保基本变量存在
            if not hasattr(self, 'cameras'):
                self.cameras = {"cam001": {"id": "cam001", "location": "默认位置"}}
            
            if not hasattr(self, 'alert_counts'):
                self.alert_counts = {cam_id: 0 for cam_id in self.cameras.keys()}
            
            if not hasattr(self, 'start_time'):
                self.start_time = datetime.now()
                
            # 告警日志文件
            self.log_dir = "data/alerts"
            os.makedirs(self.log_dir, exist_ok=True)
            self.log_file = os.path.join(self.log_dir, f"alerts_{datetime.now().strftime('%Y%m%d')}.log")
            
            # 离线目录监控
            self.offline_dirs = [os.path.join("data", "offline", cam_id) for cam_id in self.cameras.keys()]
            
            print("告警服务已以安全模式初始化 (离线模式)")
    
    def start(self):
        """启动告警服务"""
        self.running = True
        
        # 启动正常处理线程
        if not self.offline_mode and self.consumer:
            self.thread = threading.Thread(target=self._process_alerts)
            self.thread.daemon = True
            self.thread.start()
        
        # 启动离线文件检测线程
        self.offline_thread = threading.Thread(target=self._check_offline_files)
        self.offline_thread.daemon = True
        self.offline_thread.start()
        
        # 启动统计线程
        self.stats_thread = threading.Thread(target=self._print_stats)
        self.stats_thread.daemon = True
        self.stats_thread.start()
        
        print("告警服务已启动" + (" (离线模式)" if self.offline_mode else ""))
        
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()
    
    def stop(self):
        """停止告警服务"""
        self.running = False
        
        if self.thread:
            self.thread.join(timeout=2)
            
        if hasattr(self, 'offline_thread') and self.offline_thread:
            self.offline_thread.join(timeout=2)
            
        if hasattr(self, 'stats_thread') and self.stats_thread:
            self.stats_thread.join(timeout=1)
            
        if self.consumer:
            try:
                self.consumer.close()
            except:
                pass
            
        print("告警服务已停止")
    
    def _process_alerts(self):
        """处理Kafka告警消息"""
        while self.running and not self.offline_mode and self.consumer:
            try:
                # 批量获取消息（设置超时为1秒）
                message_batch = self.consumer.poll(timeout_ms=1000, max_records=5)
                
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        try:
                            # 消息已经通过deserializer自动解析为字典
                            alert = message.value
                            
                            # 处理告警
                            self._handle_alert(alert)
                            
                        except Exception as e:
                            print(f"处理告警时出错: {e}")
                            import traceback
                            traceback.print_exc()
            except Exception as e:
                print(f"消费者错误: {e}")
                time.sleep(1)  # 出错后稍等一下再继续
                
                # 如果出现Kafka错误，切换到离线模式
                if isinstance(e, (NoBrokersAvailable, KafkaError)):
                    print(f"Kafka连接丢失，切换到离线模式")
                    self.offline_mode = True
                    break
    
    def _check_offline_files(self):
        """检查离线文件目录，模拟处理"""
        while self.running:
            try:
                # 创建已处理文件列表（避免重复处理）
                processed_files = set()
                
                # 检查每个摄像头的离线目录
                for cam_id in self.cameras.keys():
                    offline_dir = os.path.join("data", "offline", cam_id)
                    
                    # 确保目录存在
                    if not os.path.exists(offline_dir):
                        continue
                    
                    # 获取目录中的jpg文件
                    try:
                        files = [f for f in os.listdir(offline_dir) if f.endswith('.jpg') and f not in processed_files]
                    except:
                        continue
                    
                    if files:
                        # 处理一个文件（模拟检测到人）
                        file = files[0]
                        file_path = os.path.join(offline_dir, file)
                        
                        # 从文件名获取时间戳
                        timestamp = datetime.now().isoformat()
                        try:
                            timestamp_str = file.split('_')[1]
                            year = int(timestamp_str[:4])
                            month = int(timestamp_str[4:6])
                            day = int(timestamp_str[6:8])
                            hour = int(timestamp_str[9:11])
                            minute = int(timestamp_str[11:13])
                            sec = int(timestamp_str[13:15])
                            timestamp = datetime(year, month, day, hour, minute, sec).isoformat()
                        except:
                            pass  # 使用当前时间
                        
                        # 模拟创建告警
                        alert = {
                            'camera_id': cam_id,
                            'timestamp': timestamp,
                            'detection': {
                                'detected': True,
                                'count': 1,
                                'humans': [{'confidence': 0.95}]
                            },
                            'offline_file': file_path
                        }
                        
                        # 处理告警
                        self._handle_alert(alert)
                        
                        # 添加到已处理列表
                        processed_files.add(file)
                        
                        # 移动文件到已处理目录
                        processed_dir = os.path.join(offline_dir, "processed")
                        os.makedirs(processed_dir, exist_ok=True)
                        
                        try:
                            # 尝试移动文件（如果文件正在被写入可能会失败）
                            new_path = os.path.join(processed_dir, file)
                            os.rename(file_path, new_path)
                        except:
                            pass  # 忽略错误，下次会再尝试
                
                # 休眠一段时间再检查
                time.sleep(5)
                
            except Exception as e:
                print(f"检查离线文件时出错: {e}")
                time.sleep(10)  # 出错后稍等久一点
    
    def _handle_alert(self, alert):
        """处理单个告警"""
        camera_id = alert["camera_id"]
        timestamp = alert["timestamp"]
        count = alert["detection"]["count"]
        
        # 更新告警计数
        self.alert_counts[camera_id] = self.alert_counts.get(camera_id, 0) + 1
        
        # 根据摄像头配置获取位置信息
        location = "未知位置"
        if camera_id in self.cameras:
            # 如果配置中有location字段就使用，否则使用摄像头ID
            location = self.cameras[camera_id].get("location", f"摄像头 {camera_id}")
        
        # 离线文件信息
        offline_info = ""
        if "offline_file" in alert:
            offline_info = f" [离线文件: {os.path.basename(alert['offline_file'])}]"
        
        # 格式化告警信息
        alert_msg = f"[{timestamp}] {location} 检测到 {count} 个人{offline_info}"
        
        # 打印到控制台
        print(f"告警: {alert_msg}")
        
        # 记录到日志文件
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(f"{alert_msg}\n")
            f.write(f"详细信息: {json.dumps(alert, ensure_ascii=False)}\n")
            f.write("-" * 80 + "\n")
    
    def _print_stats(self):
        """定期打印告警统计信息"""
        while self.running:
            time.sleep(60)  # 每分钟打印一次统计信息
            
            # 计算运行时间
            runtime = datetime.now() - self.start_time
            runtime_str = f"{runtime.days}天 {runtime.seconds//3600}小时 {(runtime.seconds//60)%60}分钟"
            
            # 打印统计信息
            print("\n告警统计信息:")
            print(f"系统运行时间: {runtime_str}")
            print(f"运行模式: {'离线' if self.offline_mode else '在线'}")
            
            total_alerts = sum(self.alert_counts.values())
            print(f"总告警数: {total_alerts}")
            
            # 按摄像头打印统计
            for cam_id, count in self.alert_counts.items():
                location = self.cameras.get(cam_id, {}).get("location", f"摄像头 {cam_id}")
                print(f"- {location}: {count} 条告警")
            
            print("-" * 40)

def main():
    try:
        service = AlertService()
        service.start()
    except Exception as e:
        print(f"告警服务启动失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 