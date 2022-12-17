import ssl
import threading
import cv2
import imutils
import kombu
import datetime
from kombu.mixins import ConsumerMixin
import requests
import os

class Alarm(ConsumerMixin):

    def __init__(self, connection, queues, camera_id):
        self.camera_id = camera_id
        self.connection = connection
        self.queues = queues

    def get_consumers(self, Consumer, channel):
        return [
            Consumer(
                queues=self.queues,
                callbacks=[self.on_message],
                )
            ]

    def on_message(self, body, message):
        print("ALARM: received message")
        _id = body['source']
        #print(self.camera_id)
        if f"camera_{self.camera_id}" == _id:
            
            print("!!! ALERT ALET ALERT !!!")
            # Remove Message From Queue
            message.ack()