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
      实时视频监控与人员检测系统
      摄像头 → 视频流 → Kafka → Flink+Yolo实时检测 → 告警/存储
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


def start_component(module_path, process_list, component_name):
    """启动系统组件"""
    print(f"启动{component_name}...")

    process = subprocess.Popen(
        [sys.executable, module_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=False,
        bufsize=1
    )

    process_list.append(process)

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
    return process


def cleanup(process_list):
    """清理并关闭所有进程"""
    print("\n正在关闭所有组件...")

    for process in process_list:
        try:
            if process.poll() is None:  # 检查进程是否仍在运行
                # 发送SIGTERM信号
                if sys.platform == 'win32':
                    process.terminate()
                else:
                    process.send_signal(signal.SIGTERM)

                # 等待进程结束
                process.wait(timeout=5)
        except:
            # 如果进程无法正常结束，强制终止
            process.kill()

    print("所有组件已关闭。")


def main():
    """主函数"""
    print_banner()

    # 检查Kafka状态
    if not check_kafka():
        user_input = input("Kafka似乎未运行。是否继续? (y/n): ")
        if user_input.lower() != 'y':
            print("退出程序。")
            return

    # 存储所有进程的列表
    processes = []

    try:
        # 启动摄像头模拟器
        camera_process = start_component(
            "src/camera/simulator.py",
            processes,
            "摄像头模拟器"
        )
        time.sleep(2)  # 等待摄像头模拟器启动

        # 启动Flink处理器
        flink_process = start_component(
            "src/flink/processor.py",
            processes,
            "Flink处理器"
        )
        time.sleep(2)  # 等待Flink处理器启动

        # 启动告警服务
        alert_process = start_component(
            "src/alert/service.py",
            processes,
            "告警服务"
        )

        print("\n所有组件已启动！按 Ctrl+C 停止系统。\n")

        # 等待用户中断
        while all(p.poll() is None for p in processes):
            time.sleep(1)

        # 检查是否有进程意外退出
        for i, p in enumerate(processes):
            if p.poll() is not None:
                component_names = ["摄像头模拟器", "Flink处理器", "告警服务"]
                print(f"警告: {component_names[i]}意外退出，返回代码: {p.poll()}")

    except KeyboardInterrupt:
        print("\n收到中断信号，正在停止系统...")

    finally:
        # 清理所有进程
        cleanup(processes)


if __name__ == "__main__":
    main() 