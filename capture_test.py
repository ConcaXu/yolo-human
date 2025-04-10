import cv2

cap = cv2.VideoCapture(0)  # 如果是视频文件，替换为文件路径
if not cap.isOpened():
    print("无法打开视频源")
else:
    ret, frame = cap.read()
    if ret:
        print("成功读取帧")
    else:
        print("无法读取帧")
cap.release()