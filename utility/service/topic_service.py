class TopicService:
    @classmethod
    def fetch_topics(cls):
        url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vR3jKfd4qkxXt5rTvXTTSsz_RYGkxcxh6-jvB9H0Mljiz-nai7xG-E63qEQ9jQhQabBrIAeJWtgKg5j/pub?gid=0&single=true&output=csv"
        import requests
        res = requests.get(url)
        return [
            topic for topic in res.text.splitlines()
        ]