# using datasets from huggingface
import os
import threading
import time
from datasets import load_dataset
import logging

from fastapi import FastAPI
logging.basicConfig(level=logging.INFO)

class YtVideosWatchdog:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.ids = self.load_latest()
        self.times = 0
        self.reload_thread = threading.Thread(target=self.reload) 

    def load_latest(self):
        filename = "latest-ids.txt"
        if not os.path.exists(filename):
            self.logger.warning(f"File {filename} not found")   
            return []
        with open(filename, "r") as f:
            ids = f.read().splitlines()
            self.logger.info(f"Loaded {len(ids)} ids from {filename}")
        return ids

    def start(self):    
        """
        Start the watchdog
        """
        self.reload_thread.start()

    def reload(self):
        # While True and reload each 1 hours
        while True:
            try:
                self.times += 1
                self.logger.info(f"Reloading the dataset {self.times} times")
                self.load()
                self.logger.info(f"Dataset reloaded with {len(self.ids)} ids")
            except Exception as e:
                self.logger.error(f"Error while reloading the dataset: {e}")
            finally:
                time.sleep(3600)
        
    def load(self):
        # load the dataset
        ds = load_dataset("jondurbin/omega-multimodal-ids", download_mode="force_redownload")
        ds = ds['train']
        self.ids = ds['youtube_id']
        with open("latest-ids.txt", "w") as f:
            f.write("\n".join(self.ids))

    def check_exists(self, id):
        return id in self.ids

    def filter_out_existing(self, ids):
        # using set for faster lookup
        same_ids = set(self.ids).intersection(set(ids))        
        return [id for id in ids if id not in same_ids]

    def shutdown(self):
        self.reload_thread.join()

yt_watchdog = YtVideosWatchdog()
app = FastAPI()

@app.on_event("startup")
async def startup_event():
    yt_watchdog.start()

# shutdown the watchdog when app is shutdown
@app.on_event("shutdown")
async def shutdown_event():
    yt_watchdog.shutdown()

@app.get("/videos/{id}/check")
async def check_id(id: str):
    return yt_watchdog.check_exists(id)


@app.post("/videos/filter")
async def filter_out_existing(req: dict):
    diff = yt_watchdog.filter_out_existing(req["ids"])
    return {
        "ids": diff,
        "total": len(yt_watchdog.ids),
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)