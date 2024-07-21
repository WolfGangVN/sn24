import json
from concurrent.futures import ThreadPoolExecutor

import requests

from utility.client.video_client import YtVideoClient
from utility.db.video_storage import VideoStorage
import logging

from utility.kafka.query_producer import Producer
logging.basicConfig(level=logging.INFO)

class Crawler:
    def __init__(self) -> None:
        self.load_queries()
        self.query_executor = ThreadPoolExecutor(max_workers=10)
        self.storage = VideoStorage()
        self.logger = logging.getLogger(__name__)
        self.YT_CHECK_SERVICE_URL = "http://localhost:8000/videos/filter"

        self.producer = Producer()

    def filter_duplicate_video_ids(self, video_ids: int):
        response = requests.post(self.YT_CHECK_SERVICE_URL, json={"ids": video_ids})
        return response.json()['ids']
    
    def load_queries(self):
        with open("augmented_queries.json") as f:
            self.queries = json.load(f)

    def check_result(self, result):
        video_ids = [video["id"] for video in result]
        # self.logger.info(f"Length of video ids: {len(video_ids)}")
        filtered_ids = self.filter_duplicate_video_ids(video_ids)
        # self.logger.info(f"Length of filtered video ids: {len(filtered_ids)}")
        return [video for video in result if video["id"] in filtered_ids]
    
    def fetch_all(self):
        futures = []
        for query in self.queries:
            topic = query["topic"]
            queries = query["queries"]

            for q in queries:
                f = self.query_executor.submit(self.fetch_videos, q)
                futures.append(f)
            result = []
            for f in futures:
                result.extend(f.result())
            result = self.check_result(result)
            self.storage.insert_videos(result, topic)
            self.logger.info(f"Inserted {len(result)} videos for {topic}")

            self.publish_data(result)
    def publish_data(self, videos):
        for video in videos:
            self.producer.publish("download", video)

    def fetch_videos(self, query):
        return YtVideoClient.search(query, limit=100)



if __name__ == "__main__":
    crawler = Crawler()
    crawler.fetch_all()
