import os
import sys
import time
import subprocess
import signal
import threading


def print_banner():
    """打印系统横幅"""
    banner = """
    ===========================================================
      实时视频监控与人员检测系统 (多摄像头版)
      摄像头 → 视频流 → Kafka → Flink+Yolo实时检测 → 告警/存储
      桌面可视化界面
    ===========================================================
    """
    print(banner)


def check_kafka():
    """检查Kafka是否已启动"""
    print("检查Kafka服务状态...")
    try:
        # 尝试从环境变量中获取Kafka地址，默认为localhost:9092
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        host, port = bootstrap_servers.split(":")

        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        result = s.connect_ex((host, int(port)))
        s.close()

        if result == 0:
            print("Kafka服务已运行。")
            return True
        else:
            print("警告: 无法连接到Kafka服务！")
            print(f"请确保Kafka服务在 {bootstrap_servers} 上运行。")
            return False
    except Exception as e:
        print(f"检查Kafka时出错: {e}")
        return False


def start_component(module_path, process_list, component_name, wait_time=3):
    """启动系统组件"""
    print(f"启动{component_name}...")

    process = subprocess.Popen(
        [sys.executable, module_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=False,
        bufsize=1
    )

    process_list.append((process, component_name))

    # 创建线程读取输出
    def read_output():
        for line in process.stdout:
            try:
                # 手动使用utf-8解码
                decoded_line = line.decode('utf-8', errors='replace')
                print(f"[{component_name}] {decoded_line.strip()}")
            except Exception as e:
                print(f"[{component_name}] 解码输出时出错: {e}")

    thread = threading.Thread(target=read_output, daemon=True)
    thread.start()

    print(f"{component_name}启动成功！")
    # 等待指定时间，确保组件完全启动
    time.sleep(wait_time)
    return process


def cleanup(process_list):
    """清理并关闭所有进程"""
    print("\n正在关闭所有组件...")

    for process, name in process_list:
        try:
            print(f"正在关闭 {name}...")
            if process.poll() is None:  # 检查进程是否仍在运行
                # 发送SIGTERM信号
                if sys.platform == 'win32':
                    process.terminate()
                else:
                    process.send_signal(signal.SIGTERM)

                # 等待进程结束
                process.wait(timeout=5)
                print(f"{name} 已关闭")
        except:
            # 如果进程无法正常结束，强制终止
            process.kill()
            print(f"{name} 被强制终止")

    print("所有组件已关闭。")


def load_config():
    """加载配置文件"""
    config_path = os.path.join("src", "config.json")
    if os.path.exists(config_path):
        with open(config_path, 'r', encoding='utf-8') as f:
            import json
            return json.load(f)
    else:
        print(f"错误: 配置文件 {config_path} 不存在")
        return None


def run_gui():
    """直接运行GUI界面"""
    try:
        # 使用subprocess直接启动GUI（使用direct_run.py）
        print("正在启动GUI界面（独立进程）...")
        
        # 使用direct_run.py启动GUI，它会正确设置路径和工作目录
        gui_process = subprocess.Popen(
            [sys.executable, os.path.join("src", "gui", "direct_run.py")],
            # 不捕获输出，让GUI直接使用标准输出
            stdout=None,
            stderr=None,
            # 使用当前的环境变量
            env=os.environ.copy(),
            # 确保在项目根目录运行
            cwd=os.path.abspath(os.path.dirname(__file__))
        )
        
        print(f"GUI进程已启动，PID: {gui_process.pid}")
        return gui_process
    except Exception as e:
        print(f"启动GUI界面时出错: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    """主函数"""
    print_banner()

    # 检查命令行参数
    if len(sys.argv) > 1 and sys.argv[1] == "--gui-only":
        # 只运行GUI部分
        gui_process = run_gui()
        if gui_process:
            try:
                gui_process.wait()  # 等待GUI进程结束
            except KeyboardInterrupt:
                gui_process.terminate()
                print("GUI已终止")
        return

    # 检查Kafka状态
    if not check_kafka():
        user_input = input("Kafka似乎未运行。是否继续? (y/n): ")
        if user_input.lower() != 'y':
            print("退出程序。")
            return

    # 加载配置
    config = load_config()
    if not config:
        return

    # 打印系统信息
    num_cameras = len(config["cameras"])
    processor_threads = config.get("processor", {}).get("num_threads", 4)
    print(f"系统配置:")
    print(f"- 摄像头数量: {num_cameras}")
    print(f"- 处理线程数: {processor_threads}")

    # 存储所有进程的列表
    processes = []

    try:
        # 启动摄像头模拟器 (支持多摄像头)
        camera_process = start_component(
            "src/camera/simulator.py",
            processes,
            "摄像头模拟器",
            wait_time=5  # 增加等待时间，确保摄像头完全初始化
        )

        # 启动Flink处理器 (多线程处理)
        flink_process = start_component(
            "src/flink/processor.py",
            processes,
            "Flink处理器",
            wait_time=3  # 等待Flink处理器启动
        )

        # 启动告警服务
        alert_process = start_component(
            "src/alert/service.py",
            processes,
            "告警服务",
            wait_time=3  # 确保告警服务完全启动
        )
        
        # 等待一段时间，让服务先生成一些数据
        print("等待服务初始化完成...")
        time.sleep(2)
        
        # 直接启动GUI界面（独立进程）
        gui_process = run_gui()
        if gui_process:
            processes.append((gui_process, "GUI界面"))
        
        print("\n所有组件已启动！")
        print("按 Ctrl+C 停止系统。\n")

        # 等待用户中断
        while all(p[0].poll() is None for p in processes):
            time.sleep(1)

        # 检查是否有进程意外退出
        for p, name in processes:
            if p.poll() is not None:
                print(f"警告: {name} 意外退出，返回代码: {p.poll()}")

    except KeyboardInterrupt:
        print("\n收到中断信号，正在停止系统...")

    finally:
        # 清理所有进程
        cleanup(processes)


if __name__ == "__main__":
    main() 