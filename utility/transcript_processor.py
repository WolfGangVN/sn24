import time
from omega import video_utils
from omega.imagebind_wrapper import ImageBind
from utility.client.video_client import YtVideoClient


class TranscriptProcessor:
    def __init__(self, transcript):
        self.transcript = transcript
        self.sentence_by_start_time, self.start_times = self.load()
        
    def process(self):
        return self.transcript.lower()
    
    def load(self):
        sentences = self.transcript.split("{|}")
        result = {}
        start_times = []
        for sentence in sentences:
            if sentence.strip():
                sentence, start_time = sentence.strip().split("|||")
                start_times.append(int(start_time))
                if int(start_time) > 0:
                    result[int(start_time)] = sentence
        start_times = sorted(start_times)
        return result, start_times
    

    def split_by_duration(self, upper_threshold_duration=100):
        result = []
        prev_end = None
        for i in range(len(self.start_times)):
            time_limit = self.start_times[i] + upper_threshold_duration
            for j in range(i + 1, len(self.start_times)):
                if self.start_times[j] >= time_limit:
                    if prev_end != j:
                        result.append(self.start_times[i:j])
                    prev_end = j
                    break
        return result
    
    def get_sentences_by_list_start_time(self, start_times=[]):
        start_times = sorted(start_times)
        is_valid = True
        for t in start_times:
            if t not in self.start_times:
                is_valid = False
        
        if is_valid:
            return ",".join([self.sentence_by_start_time[t] for t in start_times])
        return ''
    
    def get_next_start_time(self, start_time):
        for t in self.start_times:
            if t > start_time:
                return t
        return self.start_times[-1]
    
    def get_start_time_end_time(self, start_times=[]):
        return start_times[0], self.get_next_start_time(start_times[-1])

if __name__ == "__main__":
    import torch.nn.functional as F

    imagebind = ImageBind()
    
    trans = YtVideoClient.transcript()
    trans = trans['v_transcriptRaw']
    processor = TranscriptProcessor(trans)
    durations = processor.split_by_duration()

    sentences_list = []
    clip_path_list = []    
    for i in range(len(durations)):
        start_time, end_time = processor.get_start_time_end_time(durations[i])
        sentences = processor.get_sentences_by_list_start_time(durations[i])
        clip_path = video_utils.clip_video("video.mp4", start_time, end_time)
        sentences_list.append(sentences)
        clip_path_list.append(clip_path)
        print(f"{i}/ {len(durations)}")
        if i == 10:
            break
    embedings = imagebind.embed(sentences_list, clip_path_list)
    video = embedings.video
    sentence = embedings.description
    score = F.cosine_similarity(video, sentence)
    print(score)