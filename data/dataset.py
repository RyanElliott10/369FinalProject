import argparse
from typing import Dict

import pandas as pd
from nltk.sentiment import SentimentIntensityAnalyzer

analyzer = SentimentIntensityAnalyzer()


class ChunkProcesser(object):

    def __init__(self, chunksize: int = 10 ** 6, min_comm_len: int = 16):
        self.sentiment_analyzer = SentimentIntensityAnalyzer()
        self.chunksize = chunksize
        self.min_comm_len = min_comm_len
        self.desired_cols = ['body', 'gildings', 'subreddit']
        self.sentiment_cols = ['neg', 'neu', 'pos', 'compound']

    def _get_sentiment(self, sent: str) -> Dict[str, int]:
        scores = self.sentiment_analyzer.polarity_scores(sent)
        return pd.Series([scores[col] for col in self.sentiment_cols])

    def _filter_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Filters comments within a chunk to meet requirements."""
        chunk.body = chunk.dropna().body.astype('str')
        return chunk[chunk.body.map(len) >= self.min_comm_len]

    def _get_relevant_chunk_data(self, chunk: pd.DataFrame) -> pd.DataFrame:
        return chunk[self.desired_cols]

    def __call__(self, chunk: pd.DataFrame):
        rel_chunk = self._get_relevant_chunk_data(chunk).head(10)
        filtered_chunk = self._filter_chunk(rel_chunk)
        filtered_chunk[self.sentiment_cols] = filtered_chunk.body.apply(self._get_sentiment)
        print(filtered_chunk)


def process_data(path: str, chunksize: int, comm_length: int):
    processor = ChunkProcesser(min_comm_len=comm_length)
    for chunk in pd.read_csv(path, chunksize=chunksize):
        processor(chunk)


def main():
    global args
    process_data(args.datapath, chunksize=args.chunksize, comm_length=args.comm_length)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--datapath', required=True,
                        help="Path to data file")
    parser.add_argument('-c', '--chunksize', required=False, type=int, 
                        default=10 ** 6, help="Chunksize to parse data")
    parser.add_argument('-l', '--comm_length', required=False, type=int,
                        default=16, help="Minimum commnet length to be considered")
    args = parser.parse_args()
    main() 
