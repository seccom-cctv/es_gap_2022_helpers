# @Author: Rafael Direito
# @Date:   2022-10-06 11:30:52 (WEST)
# @Email:  rdireito@av.it.pt
# @Copyright: Insituto de Telecomunicações - Aveiro, Aveiro, Portugal
# @Last Modified by:   Rafael Direito
# @Last Modified time: 2022-10-07 11:34:30


from human_detection import Human_Detection_Module
import redis

# AMQP Variables
RABBIT_MQ_URL = "b-ea70ef40-9456-4646-815c-83e2a1b3079b.mq.us-east-1.amazonaws.com:5671" #"localhost:5672"
RABBIT_MQ_USERNAME = "myuser01"
RABBIT_MQ_PASSWORD = "mypassword01"
RABBIT_MQ_EXCHANGE_NAME = "human-detection-exchange"
RABBIT_MQ_QUEUE_NAME = "human-detection-queue"

#IN-MEMORY DATABASE
REDIS_PASSWORD ="mypassword123456"#"eYVX7EwVmmxKPCDmwMtyKVge8oLd2t81"

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
