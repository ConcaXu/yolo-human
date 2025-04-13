import os
import json
import sys

def ensure_config_exists():
    """确保config.json文件存在"""
    config_path = os.path.join("src", "config.json")
    
    # 检查文件是否存在
    if os.path.exists(config_path):
        print(f"配置文件已存在: {os.path.abspath(config_path)}")
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            print("配置文件内容有效")
            return True
        except Exception as e:
            print(f"配置文件存在但无法解析: {e}")
    
    # 创建默认配置
    print(f"创建默认配置文件: {os.path.abspath(config_path)}")
    default_config = {
        "kafka": {
            "bootstrap_servers": "localhost:9092",
            "video_topic": "video_frames",
            "alert_topic": "human_alerts"
        },
        "cameras": [
            {
                "id": "cam001",
                "video_source": "0",
                "frame_width": 640,
                "frame_height": 480,
                "fps": 10,
                "location": "前门"
            },
            {
                "id": "cam002",
                "video_source": "src/flink/video.mp4",
                "frame_width": 640,
                "frame_height": 480,
                "fps": 15,
                "location": "后门"
            }
        ],
        "yolo": {
            "model_path": "models/yolov8n.pt",
            "confidence": 0.5
        },
        "storage": {
            "save_frames": True,
            "output_dir": "data/detected"
        },
        "processor": {
            "num_threads": 4
        }
    }
    
    try:
        # 确保目录存在
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        
        # 写入配置文件
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(default_config, f, indent=2, ensure_ascii=False)
        
        print("配置文件创建成功")
        return True
    except Exception as e:
        print(f"创建配置文件失败: {e}")
        return False

def check_directories():
    """检查并创建必要的目录"""
    dirs = [
        "data/detected",
        "data/alerts",
        "models"
    ]
    
    for d in dirs:
        try:
            os.makedirs(d, exist_ok=True)
            print(f"目录已确保存在: {d}")
        except Exception as e:
            print(f"创建目录失败 {d}: {e}")

def main():
    print("开始检查系统配置...")
    
    # 确保配置文件存在
    if not ensure_config_exists():
        print("配置文件检查失败，请手动创建配置文件")
        return
    
    # 检查目录
    check_directories()
    
    print("配置检查完成")

if __name__ == "__main__":
    main() 