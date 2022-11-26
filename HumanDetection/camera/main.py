# @Author: Rafael Direito
# @Date:   2022-10-06 10:54:18 (WEST)
# @Email:  rdireito@av.it.pt
# @Copyright: Insituto de Telecomunicações - Aveiro, Aveiro, Portugal
# @Last Modified by:   Rafael Direito
# @Last Modified time: 2022-10-06 11:19:15

from camera import Camera

# CAMERA VARIABLES
CAMERA_ID = 1
NUM_FRAMES_PER_SECOND_TO_PROCESS = 2

# AMQP Variables
RABBIT_MQ_URL = "localhost:5672" # "b-eadcd882-eb10-4925-8457-f84a981dd896.mq.us-east-1.amazonaws.com:5671"
RABBIT_MQ_USERNAME = "myuser"
RABBIT_MQ_PASSWORD = "mypassword" # "mysecurepassword"
RABBIT_MQ_EXCHANGE_NAME = "human-detection-exchange"
RABBIT_MQ_QUEUE_NAME = "human-detection-queue"

API_URL = ""

camera = Camera(
    camera_id=CAMERA_ID,
    frames_per_second_to_process=NUM_FRAMES_PER_SECOND_TO_PROCESS,
    api_url=API_URL
    )

#camera.upload_file()

camera.attach_to_message_broker(
    broker_url=RABBIT_MQ_URL,
    broker_username=RABBIT_MQ_USERNAME,
    broker_password=RABBIT_MQ_PASSWORD,
    exchange_name=RABBIT_MQ_EXCHANGE_NAME,
    queue_name=RABBIT_MQ_QUEUE_NAME
    )

#camera.consumer.send_snapshot("samples/people-detection.mp4", 10)

# camera.transmit_video("samples/people-detection.mp4")
#camera.send_snapshot("samples/people-detection.mp4", 10)

print("End of video transmission")
