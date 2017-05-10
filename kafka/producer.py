import time
import cv2

from kafka import SimpleClient, SimpleProducer, KafkaClient, KafkaProducer

# Connect to Kafka
kafka = KafkaClient('127.0.0.1:9092')
producer = SimpleProducer(kafka)

# Assign a topic
topic = 'video'


def video_emitter(video):

    # Open the video
    video = cv2.VideoCapture(video)
    print('emitting...')
    # Read the file
    while video.isOpened:
        # Read image in each frame
        success, image = video.read()
        # Check if file has been read to the end
        if not success:
            break
        # Convert the image to png
        ret, jpeg = cv2.imencode('.png', image)
        # Convert image to bytes and send to Kafka
        producer.send_messages(topic, jpeg.tobytes())
        # Reduce CPU usage
        time.sleep(0.2)
    # Clear the capture
    video.release()
    print('Done emitting.')

if __name__ == '__main__':
    video_emitter('video.mp4')




