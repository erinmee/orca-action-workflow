# importing general Python libraries
import datetime as dt
import pytz
import json

# importing orcasound_noise libraries
from orcasound_noise.pipeline.pipeline import NoiseAnalysisPipeline
from orcasound_noise.utils import Hydrophone

hydrophone = 'BUSH_POINT'
now = dt.datetime.now(pytz.timezone('US/Pacific'))
partitioned_folder = f"data/hydrophone={hydrophone}/date={now.strftime('%Y-%m-%d')}/"
bookmark_path = f"data/{hydrophone}_bookmark.json"

# Bookmarking
class Bookmark:
    def __init__(self, hydrophone: Hydrophone, bookmark_path: str, last_processed: dt.datetime = None):
        self.hydrophone = hydrophone
        self.bookmark_path = bookmark_path
        self.last_processed = last_processed
    
    def update(self, new_time: dt.datetime):
        self.last_processed = new_time
        with open(self.bookmark_path, 'w') as f:
            json.dump({'last_processed': self.last_processed.isoformat()}, f)
    
    def load(self):
        try:
            with open(self.bookmark_path, 'r') as f:
                data = json.load(f)
                self.last_processed = dt.datetime.fromisoformat(data['last_processed'])
        except FileNotFoundError:
            self.last_processed = None

# Set Location and Resolution
# Port Townsend, 1 Hz Frequency, 60-second samples
if __name__ == '__main__':
    pipeline = NoiseAnalysisPipeline(Hydrophone.BUSH_POINT,
                                     delta_f=10, bands=None,
                                     delta_t=60, mode='safe',
                                     pqt_folder=partitioned_folder)

# Load Bookmark
bookmark = Bookmark(Hydrophone.BUSH_POINT, bookmark_path)
bookmark.load()
start_time = bookmark.last_processed or (now - dt.timedelta(hours=1))
end_time = now

# Generate parquet dataframes with noise levels for a time period
psd_path, broadband_path = pipeline.generate_parquet_file(start_time, 
                                end_time, 
                                upload_to_s3=False)
# Update bookmark
bookmark.update(end_time)