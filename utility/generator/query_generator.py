import os
import random

from openai import OpenAI

from omega.augment import AbstractAugment
import logging

from utility.service.topic_service import TopicService


def get_llm_prompt_v2(query: str, num_query: int=10) -> str:
    return f"Take the given query `{query}` and augment it to be more detailed. For example, add specific names, types, embellishments, richness. Do not make it longer than 12 words (each query). Generate at least {num_query} different versions of the query."


class OpenAIAugmentV2(AbstractAugment):
    def __init__(self, min_query=5):
        self.client = OpenAI()
        self.min_query = min_query
        logging.info("Running query augmentation with OpenAI GPT-4")

    def augment_query(self, query: str) -> str:
        response = self.client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "user",
                    "content": get_llm_prompt_v2(query, self.min_query)
                }
            ],
            temperature=0.9,
            max_tokens=1024,
            top_p=1,
        )
        # return response.choices[0].message.content.strip("\"").strip("'")
        queries = response.choices[0].message.content.strip("\"").strip("'").split("\n")
        result = []
        for query in queries:
            try:
                result.append(query.split(". ")[1])
            except Exception as e:
                print(e)
                pass
        return result
    
class QueryGenerator:
    def __init__(self):
        os.environ["OPENAI_API_KEY"] = ""
        self.openai_augmentor = OpenAIAugmentV2(min_query=10)
    

    def augment_query(self, query: str) -> str:
        return self.openai_augmentor(query)
    

if __name__ == "__main__":
    generator = QueryGenerator()
    
    TOPICS = TopicService.fetch_topics()
    result = []
    for topic in TOPICS:
        print(f"Topic: {topic}")
        queries = generator.augment_query(topic)
        result.append({
            "topic": topic,
            "queries": queries
        })

        if len(result) > 10:
            break
    
    # write to file
    import json
    with open("augmented_queries.json", "w", encoding="utf8") as f:
        json.dump(result, f, indent=4, ensure_ascii=False)