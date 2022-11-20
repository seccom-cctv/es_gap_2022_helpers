# @Author: Rafael Direito
# @Date:   2022-10-06 10:54:23 (WEST)
# @Email:  rdireito@av.it.pt
# @Copyright: Insituto de Telecomunicações - Aveiro, Aveiro, Portugal
# @Last Modified by:   Rafael Direito
# @Last Modified time: 2022-10-06 12:02:59

import cv2
import imutils
import kombu
import datetime


class Camera:

    kombu_connection = None
    kombu_exchange = None
    kombu_channel = None
    kombu_producer = None
    kombu_queue = None

    def __init__(self, camera_id, frames_per_second_to_process):
        self.camera_id = camera_id
        self.frames_per_second_to_process = frames_per_second_to_process


    def attach_to_message_broker(self, broker_url, broker_username,
                                 broker_password, exchange_name, queue_name):
        # Create Connection String
        connection_string = f"amqp://{broker_username}:{broker_password}" \
            f"@{broker_url}/"

        # Kombu Connection
        self.kombu_connection = kombu.Connection(connection_string)
        self.kombu_channel = self.kombu_connection.channel()

        # Kombu Exchange
        self.kombu_exchange = kombu.Exchange(
            name=exchange_name,
            type="direct",
            delivery_mode=1
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
                kombu.binding(self.kombu_exchange, routing_key='snapshot')
            ]
        )
        self.kombu_queue.maybe_bind(self.kombu_connection)
        self.kombu_queue.declare()

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
        print('Start snapshot...')
        time_now = datetime.datetime.now()

        frame_count = 0
        frame_id = 0
        while video.isOpened():

            # check is True if reading was successful
            check, frame = video.read()

            if check:
                if frame_count % frame_step == 0 and frame_count >= start_frame:

                    headers = {
                            "source": f"camera_{self.camera_id}",
                            "timestamp": str(time_now),
                            "frame_count": frame_count,
                            "frame_id": frame_id,
                            "end": frame_count - start_frame >= 10,
                            "fps": video_fps
                        }

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
                        headers=headers,
                        routing_key='snapshot'
                    )
                    print(f"[Camera {self.camera_id}] Sent a frame to " +
                          "intrusion-api " +
                          f"(frame_number={frame_count}, " +
                          f"frame_timestamp={time_now})")

                    frame_id += 1
                    #key = cv2.waitKey(1)
                    #if key == ord('q'):
                    #    break
                if frame_count % frame_step == 0 and frame_count - start_frame > 10:
                    break
            else:
                break

            frame_count += 1

        print("End snapshot...")


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
