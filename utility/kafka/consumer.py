from concurrent.futures import ThreadPoolExecutor
import json
import logging.config
import os
import time
import traceback
from kafka import KafkaConsumer

from omega import video_utils
from omega.imagebind_wrapper import ImageBind
from utility.client.video_client import YtVideoClient
from utility.db.video_storage import VideoStorage
from utility.kafka.query_producer import Producer 
import threading
import logging
import torch.nn.functional as F

from utility.transcript_processor import TranscriptProcessor
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# write annotation to log duration process of function
def log_duration(func):
    def wrapper(*args, **kwargs):
        import time
        start = time.time()
        result = func(*args, **kwargs)
        logging.info(f"Function {func.__name__} executed in {time.time() - start}")
        # print(f"Execution time: {time.time() - start}")
        return result
    return wrapper

class Consumer:
    def __init__(self, topic: str) -> None:
        self.download_consumer = KafkaConsumer(topic, bootstrap_servers='localhost:9092', auto_offset_reset='latest', value_deserializer=lambda v: json.loads(v.decode('utf-8')))
        self.embeddings_comsumer = KafkaConsumer("embeddings", bootstrap_servers='localhost:9092', auto_offset_reset='latest', value_deserializer=lambda v: json.loads(v.decode('utf-8')))
        self.executor = ThreadPoolExecutor(max_workers=20)
        self.FUTURES =[]
        self.storage = VideoStorage()
        self.producer = Producer()
        self.logger = logging.getLogger(__name__)
        
        self.embeddings_executor = ThreadPoolExecutor(max_workers=1)
        self.imagebind = ImageBind()

    def embeddings_consume(self):
        self.logger.info("Start consuming embeddings")
        for message in self.embeddings_comsumer:
            print(f"Consumed {message.value} from {message.topic}")
            self.embeddings_video(message.value)
    
    @log_duration
    def embeddings_video(self, data):
        video_id = data["id"]
        download_path = f"videos/{video_id}.mp4"
        self.logger.info(f"Start embeddings video {video_id}")
        transcript = YtVideoClient.transcript(video_id)
        transcript_processor = TranscriptProcessor(transcript["v_transcriptRaw"])
        durations = transcript_processor.split_by_duration()
        self.logger.info(f"Lenght of durations: {len(durations)}")
        sentences_list = []
        clip_path_list = []
        for i in range(len(durations)):
            start_time, end_time = transcript_processor.get_start_time_end_time(durations[i])
            sentences = transcript_processor.get_sentences_by_list_start_time(durations[i])
            clip_path = video_utils.clip_video(download_path, start_time, end_time)
            sentences_list.append(sentences)
            clip_path_list.append(clip_path)
            print(f"{i}/ {len(durations)}")
        
        self.logger.info("finish clip video")
        # calculate score by batch of each 10 videos
        scores = []
        for i in range(0, len(sentences_list), 10):
            self.logger.info(f"Embeddings {i} to {i+10}")
            sentences_batch = sentences_list[i:i+10]
            clip_path_batch = clip_path_list[i:i+10]
            try:
                embedings = self.imagebind.embed(sentences_batch, clip_path_batch)
                video = embedings.video
                sentence = embedings.description
                sub_scores = F.cosine_similarity(video, sentence)
                scores.extend(sub_scores.tolist())
            except:
                traceback.print_exc()
        # get list index of top 5 scores
        top_5_index = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:5]
        top_5_sentences = [sentences_list[i] for i in top_5_index]
        top_5_duration = [durations[i] for i in top_5_index]
        self.logger.info(f"Top 5 sentences: {top_5_sentences}")
        self.logger.info(f"Top 5 duration: {top_5_duration}")

    def donwload_consume(self):
        self.logger.info("Start consuming download")
        for message in self.download_consumer:
            print(f"Consumed {message.value} from {message.topic}")
            self.process_data(message.value)
    
    def consume(self):
        download_thread = threading.Thread(target=self.donwload_consume)
        embeddings_thread = threading.Thread(target=self.embeddings_consume)
        download_thread.start()
        embeddings_thread.start()
        download_thread.join()
        embeddings_thread.join()

    def _process_data(self, data):
        video_id = data["id"]
        download_path, video_id = YtVideoClient.download_video_by_id(video_id, f"videos/{video_id}.mp4")
        self.storage.update_download_path(video_id, download_path)
        # self.producer.publish("embeddings", data)
        self.embeddings_executor.submit(self.embeddings_video, data)


    @log_duration
    def process_data(self, data):
        future = self.executor.submit(self._process_data, data)
        self.FUTURES.append(future)
        if len(self.FUTURES) > 0:
            self.logger.info(f"Waiting for {len(self.FUTURES)} to finish")
            for i, f in enumerate(self.FUTURES):
                f.result()
                self.logger.info(f"Finished {i}/{len(self.FUTURES)}")
            self.FUTURES = []
            time.sleep(100000)

if __name__ == "__main__":
    consumer = Consumer("download")
    consumer.consume()
    # consumer.embeddings_video({"id": "_v0hp0caNEQ"})