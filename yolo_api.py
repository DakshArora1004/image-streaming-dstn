from flask import Flask, request, jsonify
import torch
from PIL import Image
import io

# Load YOLOv5 model
model = torch.hub.load('ultralytics/yolov5', 'yolov5s', pretrained=True)

app = Flask(__name__)

@app.route('/detect', methods=['POST'])
def detect():
    file = request.files['image']
    image = Image.open(io.BytesIO(file.read()))

    # Perform inference
    results = model(image)
    detections = results.pandas().xyxy[0]

    # Count cars
    car_count = detections[detections['name'] == 'car'].shape[0]
    return jsonify({"car_count": car_count})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

