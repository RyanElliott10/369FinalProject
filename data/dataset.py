import argparse
from typing import Dict
import warnings

warnings.simplefilter(action='ignore', category=Warning) # Not the best
import pandas as pd
from nltk.sentiment import SentimentIntensityAnalyzer

analyzer = SentimentIntensityAnalyzer()


class ChunkProcesser(object):

    def __init__(self, output_path: str, chunksize: int = 10 ** 6, min_comm_len: int = 16):
        self.sentiment_analyzer = SentimentIntensityAnalyzer()
        self.output_path = output_path
        self.chunksize = chunksize
        self.min_comm_len = min_comm_len
        self.desired_cols = ['body', 'gildings', 'subreddit']
        self.sentiment_cols = ['neg', 'neu', 'pos', 'compound']
        self.has_written = False

    def _get_sentiment(self, sent: str) -> Dict[str, int]:
        scores = self.sentiment_analyzer.polarity_scores(sent)
        return pd.Series([scores[col] for col in self.sentiment_cols])

    def _filter_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Filters comments within a chunk to meet requirements."""
        chunk.body = chunk.dropna().body.astype('str')
        return chunk[chunk.body.map(lambda el: len(str(el))) >= self.min_comm_len]

    def _get_relevant_chunk_data(self, chunk: pd.DataFrame) -> pd.DataFrame:
        return chunk[self.desired_cols]

    def _save_chunk(self, chunk: pd.DataFrame):
        """Writes a chunk to the output CSV. First write should overwrite the
        file's current contens while susbequent writes should simply append to
        the file.
        """
        if self.has_written:
            chunk.to_csv(self.output_path, mode='a', header=False)
        else:
            chunk.to_csv(self.output_path)
            self.has_written = True

    def __call__(self, chunk: pd.DataFrame):
        rel_chunk = self._get_relevant_chunk_data(chunk)
        filtered_chunk = self._filter_chunk(rel_chunk)
        filtered_chunk[self.sentiment_cols] = filtered_chunk.body.apply(
            self._get_sentiment)
        # Since we have such a massive dataset, I think we should selectively
        # remove datapoints to try to get an even spread of neg, neu, and pos
        # datapoints
        self._save_chunk(filtered_chunk)


def process_data(path: str, output_path: str, chunksize: int, comm_length: int):
    processor = ChunkProcesser(output_path, min_comm_len=comm_length)
    for (i, chunk) in enumerate(pd.read_csv(path, chunksize=chunksize)):
        processor(chunk)
        if i == 50:
            exit()


def main():
    global args
    process_data(args.datapath, args.output_path, chunksize=args.chunksize,
                 comm_length=args.comm_length)


if __name__ == '__main__':
    # Defaults generate ~50MB of data
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--datapath', required=True,
                        help="Path to data file")
    parser.add_argument('-o', '--output_path', required=True,
                        help="Output file for dataset")
    parser.add_argument('-c', '--chunksize', required=False, type=int,
                        default=10000, help="Chunksize to parse data")
    parser.add_argument('-l', '--comm_length', required=False, type=int,
                        default=16,
                        help="Minimum commnet length to be considered")
    args = parser.parse_args()
    main()
