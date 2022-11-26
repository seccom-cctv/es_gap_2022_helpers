# @Author: Rafael Direito
# @Date:   2022-10-06 10:54:23 (WEST)
# @Email:  rdireito@av.it.pt
# @Copyright: Insituto de Telecomunicações - Aveiro, Aveiro, Portugal
# @Last Modified by:   Rafael Direito
# @Last Modified time: 2022-10-06 12:02:59

import threading
import cv2
import imutils
import kombu
import datetime
import ssl
from kombu.mixins import ConsumerMixin
import requests
import os


class myConsumer(ConsumerMixin):

    def __init__(self, connection, queues, camera_id, camera, frames_per_second_to_process, api_url):
        self.camera_id = camera_id
        self.connection = connection
        self.queues = queues
        self.camera = camera
        self.frames_per_second_to_process = frames_per_second_to_process
        self.url = api_url

    def get_consumers(self, Consumer, channel):
        return [
            Consumer(
                queues=self.queues,
                callbacks=[self.on_message],
                )
            ]

    def on_message(self, body, message):
        # Get message headers' information
        #print("M", message)
        #print(body)
        _id = body['source']
        #print(self.camera_id)
        if f"camera_{self.camera_id}" == _id:
            init_frame = int(body['frame_id'])
            name = self.send_snapshot("samples/people-detection.mp4", init_frame)

            print(f"POST VIDEO {name}")
            with open(name, "rb") as vf:
                data = {'key':'metadata','timeDuration':120}
                files = {'file': vf}
                x = requests.post(self.url, files=files, json=data)

            # Delete tmep file
            os.remove(name)

            # Remove Message From Queue
            message.ack()

    def send_snapshot(self, video_path, start_frame):
        video = cv2.VideoCapture(video_path)

        # Check if the video exists
        check, frame = video.read()
        if not check:
            print("Video Not Found. Please Enter a Valid Path (Full path of " +
                  "Video Should be Provided).")
            return

        # Compute the frame step
        video_fps = video.get(cv2.CAP_PROP_FPS)
        frame_step = video_fps/self.frames_per_second_to_process
        print('Start snapshot... Frame: ', start_frame)
        time_now = datetime.datetime.now()

        MINUTES = 1*60

        end_frame = MINUTES*video_fps + start_frame
        start_frame = start_frame - MINUTES*video_fps if start_frame - MINUTES*video_fps > 0 else 0
        print(f"FROM {start_frame}, END: {end_frame}, FPS: {video_fps}")
        frame_count = 0
        frame_id = 0
        out = None
        name = f"tempfiles/{self.camera_id}-{start_frame}-{end_frame}.mp4"
        while video.isOpened():

            # check is True if reading was successful
            check, frame = video.read()

            if check:
                if frame_count % frame_step == 0 and frame_count >= start_frame:

                    # Resize frame
                    frame = imutils.resize(
                        frame,
                        width=min(800, frame.shape[1])
                    )

                    # Encode to JPEG
                    result, imgencode = cv2.imencode(
                        '.jpg',
                        frame,
                        [int(cv2.IMWRITE_JPEG_QUALITY), 90]
                    )

                    frame_seconds = frame_count/video_fps
                    time_now += datetime.timedelta(seconds=frame_seconds)
                    height, width, _ = frame.shape
                    print(height, width)

                    if not out:
                        out = cv2.VideoWriter(name,cv2.VideoWriter_fourcc(*'DIVX'), video_fps, (width, height))

                    out.write(frame)

                    # print(f"[Camera {self.camera_id}] Sent a frame to " +
                    #       "intrusion-api " +
                    #       f"(frame_number={frame_count}, " +
                    #       f"frame_timestamp={time_now})")

                    frame_id += 1
                if frame_count > end_frame:
                    break
            else:
                break

            frame_count += 1

        if out:
            out.release()
        print("End snapshot...")
        return name

class Camera:

    kombu_connection = None
    kombu_exchange = None
    kombu_channel = None
    kombu_producer = None
    kombu_queue = None

    def __init__(self, camera_id, frames_per_second_to_process, api_url):
        self.camera_id = camera_id
        self.frames_per_second_to_process = frames_per_second_to_process
        self.url = api_url

    def attach_to_message_broker(self, broker_url, broker_username,
                                 broker_password, exchange_name, queue_name):
        # Create Connection String
        connection_string = f"amqp://{broker_username}:{broker_password}" \
            f"@{broker_url}/"

        #ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        #ssl_context.set_ciphers('ECDHE+AESGCM:!ECDSA')

        # Kombu Connection
        self.kombu_connection = kombu.Connection(connection_string)
        self.kombu_channel = self.kombu_connection.channel()

        # Kombu Exchange
        self.kombu_exchange = kombu.Exchange(
            name=exchange_name,
            type="direct",
            delivery_mode=1
        )

        cons_exchange = kombu.Exchange(
            name="hdm-to-camera",
            type="direct",
        )

        # Kombu Producer
        self.kombu_producer = kombu.Producer(
            exchange=self.kombu_exchange,
            channel=self.kombu_channel
        )

        # Kombu Queue
        self.kombu_queue = kombu.Queue(
            name=queue_name,
            bindings=[
                kombu.binding(self.kombu_exchange, routing_key='hdm'),
            ]
        )
        queue2 = kombu.Queue(
                    name="human-detection-queue-resp",
                    exchange=cons_exchange,
                    bindings=[
                        kombu.binding(cons_exchange, routing_key='hdm-resp1'),
                    ],
                )

        self.kombu_queue.maybe_bind(self.kombu_connection)
        self.kombu_queue.declare()

        queue2.maybe_bind(self.kombu_connection)
        queue2.declare()

        self.consumer = myConsumer(
            connection=self.kombu_connection,
            queues=queue2,
            camera_id=self.camera_id,
            camera=self,
            frames_per_second_to_process=self.frames_per_second_to_process,
            api_url=self.url
        )
        x = threading.Thread(target=self.consumer.run)
        x.start()
        self.transmit_video("samples/people-detection.mp4")
        x.join()

    def transmit_video(self, video_path):
        video = cv2.VideoCapture(video_path)

        # Check if the video exists
        check, frame = video.read()
        if not check:
            print("Video Not Found. Please Enter a Valid Path (Full path of " +
                  "Video Should be Provided).")
            return

        # Compute the frame step
        video_fps = video.get(cv2.CAP_PROP_FPS)
        frame_step = video_fps/self.frames_per_second_to_process
        print('Detecting people...')
        time_now = datetime.datetime.now()

        frame_count = 0
        frame_id = 0
        while video.isOpened():

            # check is True if reading was successful
            check, frame = video.read()

            if check:
                if frame_count % frame_step == 0:

                    # Resize frame
                    frame = imutils.resize(
                        frame,
                        width=min(800, frame.shape[1])
                    )

                    # Encode to JPEG
                    result, imgencode = cv2.imencode(
                        '.jpg',
                        frame,
                        [int(cv2.IMWRITE_JPEG_QUALITY), 90]
                    )

                    frame_seconds = frame_count/video_fps
                    time_now += datetime.timedelta(seconds=frame_seconds)

                    # send a message
                    self.kombu_producer.publish(
                        body=imgencode.tobytes(),
                        content_type='image/jpeg',
                        content_encoding='binary',
                        headers={
                            "source": f"camera_{self.camera_id}",
                            "timestamp": str(time_now),
                            "frame_count": frame_count,
                            "frame_id": frame_id
                        },
                        routing_key='hdm'
                    )
                    print(f"[Camera {self.camera_id}] Sent a frame to " +
                          "the human-detection module " +
                          f"(frame_number={frame_count}, " +
                          f"frame_timestamp={time_now})")

                    frame_id += 1
                    #key = cv2.waitKey(1)
                    #if key == ord('q'):
                    #    break
            else:
                break

            frame_count += 1
