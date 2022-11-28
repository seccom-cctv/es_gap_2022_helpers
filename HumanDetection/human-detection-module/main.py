# @Author: Rafael Direito
# @Date:   2022-10-06 11:30:52 (WEST)
# @Email:  rdireito@av.it.pt
# @Copyright: Insituto de Telecomunicações - Aveiro, Aveiro, Portugal
# @Last Modified by:   Rafael Direito
# @Last Modified time: 2022-10-07 11:34:30


from human_detection import Human_Detection_Module
import os
from dotenv import load_dotenv
from pathlib import Path

path = Path("../.env")
load_dotenv(path)
# AMQP Variables
RABBIT_MQ_URL = os.getenv("RABBIT_MQ_URL")
RABBIT_MQ_USERNAME = os.getenv("RABBIT_MQ_USERNAME")
RABBIT_MQ_PASSWORD = os.getenv("RABBIT_MQ_PASSWORD")
RABBIT_MQ_EXCHANGE_NAME = os.getenv("RABBIT_MQ_EXCHANGE_NAME")
RABBIT_MQ_QUEUE_NAME = os.getenv("RABBIT_MQ_QUEUE_NAME")

#IN-MEMORY DATABASE
REDIS_PASSWORD = "eYVX7EwVmmxKPCDmwMtyKVge8oLd2t81"

# OUTPUT
OUTPUT_DIR = "intruders"

human_detection_worker = Human_Detection_Module(OUTPUT_DIR, REDIS_PASSWORD)

human_detection_worker.start_processing(
    broker_url=RABBIT_MQ_URL,
    broker_username=RABBIT_MQ_USERNAME,
    broker_password=RABBIT_MQ_PASSWORD,
    exchange_name=RABBIT_MQ_EXCHANGE_NAME,
    queue_name=RABBIT_MQ_QUEUE_NAME
    )
