import requests
import logging
from clint.textui import progress
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

class YtVideoClient:
    BASE_URL = "https://spmining.leaftensor.com/api/sn24"
    LIMIT_TIMES = 10
    logger = logging.getLogger(__name__)
    
    @classmethod
    def _search(cls, query, token=None):
        url = f"{cls.BASE_URL}/search"
        params = {"query_yt": query}
        if token:
            params["token"] = token
        # cls.logger.info(f"Fetching {url} with {params}")
        response = requests.get(url, params=params)
        # cls.logger.info(f"Response {response.json()}")
        return response.json()

    @classmethod
    def search(cls, query, limit=10):
        token = None
        results = []
        times = 0
        cls.logger.info(f"Searching {query} with limit {limit}")
        while len(results) < limit:
            times += 1
            try:
                if times == cls.LIMIT_TIMES:
                    break
                response = cls._search(query, token)
                results.extend(response.get("videos", []))
                token = response.get("next", None)
                if not token:
                    break
            except Exception as e:
                cls.logger.error(e)
                break
        return results

    @classmethod
    def get_link_download(cls, video_id):
        try:
            response = requests.get(f"{cls.BASE_URL}/video-link", params={
                "v_id": video_id
            })
            return response.json()["data"]
        except Exception as e:
            cls.logger.error(e)
            return None
        
    @classmethod
    def transcript(cls, video_id="W6NZfCO5SIk"):
        try:
            response = requests.get(f"{cls.BASE_URL}/transcript", params={
                "v_id": video_id    
            })   
            return response.json()['video'][0]
        except Exception as e:
            cls.logger.error(e)
            return None
    
    @classmethod
    def download_video_by_id(cls, video_id, path):
        import pathlib
        pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
        link = cls.get_link_download(video_id)
        if not link:
            return
        url = link["url_v360_primary"]   
        # download video from url with requests
        video = requests.get(url, stream=True)
        total_length = int(video.headers.get('content-length'))
        # show progress bar
        with open(path, "wb") as f:
            for chunk in progress.bar(video.iter_content(chunk_size=1024), expected_size=(total_length/1024) + 1): 
                if chunk:
                    f.write(chunk)
                    f.flush()
        return path, video_id
    
if __name__ == "__main__":

    link = YtVideoClient.get_link_download("W6NZfCO5SIk")
    print(link)
    url = link["url_v360_primary"]   
    # download video from url with requests
    video = requests.get(url, stream=True)
    print(url)
    total_length = int(video.headers.get('content-length'))
    # show progress bar
    with open("video.mp4", "wb") as f:
        for chunk in progress.bar(video.iter_content(chunk_size=1024), expected_size=(total_length/1024) + 1): 
            if chunk:
                f.write(chunk)
                f.flush()