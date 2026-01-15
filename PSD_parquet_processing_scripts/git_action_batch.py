# importing general Python libraries
import datetime as dt
import pytz

# importing orcasound_noise libraries
from orcasound_noise.pipeline.pipeline import NoiseAnalysisPipeline
from orcasound_noise.utils import Hydrophone


# Set Location and Resolution
# Port Townsend, 1 Hz Frequency, 60-second samples
if __name__ == '__main__':
    pipeline = NoiseAnalysisPipeline(Hydrophone.BUSH_POINT,
                                     delta_f=10, bands=None,
                                     delta_t=60, mode='safe',
                                     pqt_folder='./data/parquet_files/')




# Generate parquet dataframes with noise levels for a time period
now = dt.datetime.now(pytz.timezone('US/Pacific'))

psd_path, broadband_path = pipeline.generate_parquet_file(now - dt.timedelta(hours = 1), 
                                                          now, 
                                                          upload_to_s3=False)