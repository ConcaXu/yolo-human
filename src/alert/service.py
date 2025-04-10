import os
import sys
import time
import json
import threading
from confluent_kafka import Consumer
from datetime import datetime

# 添加项目根目录到系统路径
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.utils import load_config

class AlertService:
    def __init__(self):
        """初始化告警服务"""
        self.config = load_config()
        self.kafka_config = self.config["kafka"]
        
        # 初始化Kafka消费者
        self.consumer = Consumer({
            'bootstrap.servers': self.kafka_config["bootstrap_servers"],
            'group.id': 'alert-service',
            'auto.offset.reset': 'latest'
        })
        
        self.running = False
        self.thread = None
        
        # 告警日志文件
        self.log_dir = "data/alerts"
        os.makedirs(self.log_dir, exist_ok=True)
        self.log_file = os.path.join(self.log_dir, f"alerts_{datetime.now().strftime('%Y%m%d')}.log")
        
        print("告警服务已初始化")
    
    def start(self):
        """启动告警服务"""
        self.running = True
        self.thread = threading.Thread(target=self._process_alerts)
        self.thread.daemon = True
        self.thread.start()
        
        # 订阅Kafka主题
        self.consumer.subscribe([self.kafka_config["alert_topic"]])
        
        print("告警服务已启动")
        
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()
    
    def stop(self):
        """停止告警服务"""
        self.running = False
        if self.thread:
            self.thread.join()
        self.consumer.close()
        print("告警服务已停止")
    
    def _process_alerts(self):
        """处理告警消息"""
        while self.running:
            msg = self.consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                print(f"消费者错误: {msg.error()}")
                continue
            
            try:
                # 解析告警消息
                alert = json.loads(msg.value().decode('utf-8'))
                
                # 处理告警
                self._handle_alert(alert)
                
            except Exception as e:
                print(f"处理告警时出错: {e}")
    
    def _handle_alert(self, alert):
        """处理单个告警"""
        camera_id = alert["camera_id"]
        timestamp = alert["timestamp"]
        count = alert["detection"]["count"]
        
        # 格式化告警信息
        alert_msg = f"[{timestamp}] 摄像头 {camera_id} 检测到 {count} 个人"
        
        # 打印到控制台
        print(f"告警: {alert_msg}")
        
        # 记录到日志文件
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(f"{alert_msg}\n")
            f.write(f"详细信息: {json.dumps(alert, ensure_ascii=False)}\n")
            f.write("-" * 80 + "\n")

def main():
    service = AlertService()
    service.start()

if __name__ == "__main__":
    main() 