# importing general Python libraries
import datetime as dt
import pytz
import json
import argparse

# importing orcasound_noise libraries
from orcasound_noise.pipeline.pipeline import NoiseAnalysisPipeline
from orcasound_noise.utils import Hydrophone
from orcasound_noise.utils.file_connector import S3FileConnector


# Bookmarking
class Bookmark:
    def __init__(self, bookmark_path: str, last_processed: dt.datetime = None):
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


def process_audio_data(start_time: dt.datetime, end_time: dt.datetime, 
                       hydrophone: Hydrophone, bookmark: Bookmark, file_connector: S3FileConnector):
    """
    Process audio data for the given time range, partitioning by day if necessary.
    
    Args:
        start_time: Start datetime for processing
        end_time: End datetime for processing
        hydrophone: Name of the hydrophone location
        bookmark: Bookmark object for tracking progress
    """
    # If start and end time are on different days, partition the processing
    if start_time.day != end_time.day:
        start_date = start_time.date()
        end_date = end_time.date()
        dates_array = [start_date + dt.timedelta(days=x) for x in range((end_date - start_date).days + 1)]
        for i in range(len(dates_array)):
            intermediate_end = dt.datetime(dates_array[i].year, dates_array[i].month, dates_array[i].day, tzinfo=start_time.tzinfo) + dt.timedelta(days=1)
            partitioned_folder = f"hydrophone={hydrophone.value.name}/date={dates_array[i].strftime('%Y-%m-%d')}"
            if i == len(dates_array) - 1:
                intermediate_end = end_time
            # Set Location and Resolution
            pipeline = NoiseAnalysisPipeline(hydrophone,
                                             delta_f=1, bands=12,
                                             delta_t=1, mode='safe',
                                             pqt_folder=partitioned_folder)
            
            # Generate parquet dataframes with noise levels for a time period
            psd_path, broadband_path = pipeline.generate_parquet_file(start_time, 
                                        intermediate_end, 
                                        upload_to_s3=False)
            
            file_connector.upload_file(psd_path, start_time, intermediate_end, secs_per_sample=1, delta_hz=1, octave_bands=12, s3_folder_override='ambient-sound-analysis/type=PSD/' + partitioned_folder)
            file_connector.upload_file(broadband_path, start_time, intermediate_end, secs_per_sample=1, is_broadband=True, s3_folder_override='ambient-sound-analysis/type=BB/' + partitioned_folder)

            # Update bookmark
            bookmark.update(intermediate_end)
            start_time = intermediate_end
    else:
        partitioned_folder = f"hydrophone={hydrophone.value.name}/date={start_time.strftime('%Y-%m-%d')}"
        pipeline = NoiseAnalysisPipeline(hydrophone,
                                         delta_f=1, bands=12,
                                         delta_t=1, mode='safe',
                                         pqt_folder=partitioned_folder)

        # Generate parquet dataframes with noise levels for a time period
        psd_path, broadband_path = pipeline.generate_parquet_file(start_time, 
                                        end_time, 
                                        upload_to_s3=False)
        file_connector.upload_file(psd_path, start_time, end_time, secs_per_sample=1, delta_hz=1, octave_bands=12, s3_folder_override='ambient-sound-analysis/type=PSD/' + partitioned_folder)
        file_connector.upload_file(broadband_path, start_time, end_time, secs_per_sample=1, is_broadband=True, s3_folder_override='ambient-sound-analysis/type=BB/' + partitioned_folder)
    
    # Update bookmark with final end time
    bookmark.update(end_time)


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Process hydrophone audio data and generate PSD parquet files.')
    parser.add_argument('--hydrophone', 
                       type=str, 
                       default='bush_point',
                       choices=[h.value.name for h in Hydrophone][1:],
                       help='Hydrophone location to process')
    
    args = parser.parse_args()
    hydrophone = Hydrophone[args.hydrophone.upper()]
    
    now = dt.datetime.now(pytz.timezone('US/Pacific'))
    bookmark_path = f"data/{hydrophone.value.name}_bookmark.json"

    # Load Bookmark
    bookmark = Bookmark(bookmark_path)
    bookmark.load()
    start_time = bookmark.last_processed or (now - dt.timedelta(hours=1))
    end_time = now

    file_connector = S3FileConnector(hydrophone)
    process_audio_data(start_time, end_time, hydrophone, bookmark, file_connector)


if __name__ == '__main__':
    main()