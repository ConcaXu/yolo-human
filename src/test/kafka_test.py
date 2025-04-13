import os
import sys
import json
import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError


def load_project_config():
    """加载项目配置"""
    try:
        config_path = os.path.join("..\config.json")
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                return config
        else:
            print(f"错误: 配置文件 {config_path} 不存在")
            return None
    except Exception as e:
        print(f"加载配置文件出错: {e}")
        return None


def test_project_kafka():
    """测试项目的Kafka配置"""
    config = load_project_config()
    if not config:
        return False

    kafka_config = config.get("kafka", {})
    servers = kafka_config.get("bootstrap_servers", "localhost:9092")
    video_topic = kafka_config.get("video_topic", "video_frames")
    alert_topic = kafka_config.get("alert_topic", "human_alerts")

    print("\n======= 项目Kafka配置测试 =======")
    print(f"Kafka服务器: {servers}")
    print(f"视频帧主题: {video_topic}")
    print(f"告警主题: {alert_topic}")

    # 测试连接
    try:
        # 测试简单连接
        import socket
        host, port = servers.split(":")
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        result = s.connect_ex((host, int(port)))
        s.close()

        if result != 0:
            print(f"❌ TCP连接失败: 无法连接到 {servers}，错误代码: {result}")
            return False

        print(f"✅ TCP连接成功: 可以连接到 {servers}")

        # 创建生产者
        producer = KafkaProducer(
            bootstrap_servers=servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            # 修复超时设置问题
            request_timeout_ms=20000,
            api_version_auto_timeout_ms=20000
        )

        # 创建消费者
        consumer = KafkaConsumer(
            bootstrap_servers=servers,
            auto_offset_reset='latest',
            group_id='test-group',
            # 修复超时设置问题
            request_timeout_ms=20000,
            session_timeout_ms=10000
        )

        # 列出可用主题
        available_topics = consumer.topics()
        print(f"\n可用主题列表: {available_topics}")

        # 检查项目主题是否存在
        if video_topic in available_topics:
            print(f"✅ 视频帧主题 '{video_topic}' 已存在")
        else:
            print(f"❌ 视频帧主题 '{video_topic}' 不存在")

        if alert_topic in available_topics:
            print(f"✅ 告警主题 '{alert_topic}' 已存在")
        else:
            print(f"❌ 告警主题 '{alert_topic}' 不存在")

        # 测试发送消息
        print("\n正在发送测试消息...")
        test_message = {
            "test": True,
            "message": "这是一条测试消息",
            "timestamp": time.time()
        }

        # 发送到视频帧主题
        producer.send(video_topic, test_message)
        # 发送到告警主题
        producer.send(alert_topic, test_message)
        producer.flush()

        print("✅ 消息发送成功")

        # 关闭连接
        producer.close()
        consumer.close()

        print("\n测试结果: Kafka连接正常，可以正常发送消息")
        return True

    except NoBrokersAvailable:
        print("❌ 错误: 无法连接Kafka，找不到可用的Broker")
        print("检查事项:")
        print("1. Kafka服务是否已启动")
        print("2. 配置的服务器地址是否正确")
        print(f"3. 服务器 {servers} 是否可以从当前网络访问")
        return False
    except KafkaError as e:
        print(f"❌ Kafka错误: {e}")
        print("\n检查事项:")
        print("1. 检查Kafka服务版本")
        print("2. 查看Kafka服务器日志")
        return False
    except Exception as e:
        print(f"❌ 测试出错: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    test_project_kafka()