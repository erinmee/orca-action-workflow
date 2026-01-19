import unittest
from unittest.mock import Mock, patch, MagicMock, call
import datetime as dt
import pytz
import json
import tempfile
import os

# Import the module to test
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../PSD_parquet_processing_scripts'))
from git_action_batch import Bookmark, process_audio_data


class TestBookmark(unittest.TestCase):
    """Test the Bookmark class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()
        self.bookmark_path = os.path.join(self.test_dir, 'test_bookmark.json')
        
    def tearDown(self):
        """Clean up test files."""
        if os.path.exists(self.bookmark_path):
            os.remove(self.bookmark_path)
        os.rmdir(self.test_dir)
    
    def test_bookmark_initialization(self):
        """Test bookmark initialization."""
        bookmark = Bookmark('BUSH_POINT', self.bookmark_path)
        self.assertEqual(bookmark.hydrophone, 'BUSH_POINT')
        self.assertEqual(bookmark.bookmark_path, self.bookmark_path)
        self.assertIsNone(bookmark.last_processed)
    
    def test_bookmark_update(self):
        """Test bookmark update writes to file."""
        bookmark = Bookmark('BUSH_POINT', self.bookmark_path)
        test_time = dt.datetime(2026, 1, 15, 10, 30, tzinfo=pytz.UTC)
        bookmark.update(test_time)
        
        # Check that file was created and contains correct data
        self.assertTrue(os.path.exists(self.bookmark_path))
        with open(self.bookmark_path, 'r') as f:
            data = json.load(f)
        self.assertEqual(data['last_processed'], test_time.isoformat())
    
    def test_bookmark_load(self):
        """Test bookmark load reads from file."""
        # Create a bookmark file manually
        test_time = dt.datetime(2026, 1, 15, 10, 30, tzinfo=pytz.UTC)
        with open(self.bookmark_path, 'w') as f:
            json.dump({'last_processed': test_time.isoformat()}, f)
        
        # Load the bookmark
        bookmark = Bookmark('BUSH_POINT', self.bookmark_path)
        bookmark.load()
        
        self.assertEqual(bookmark.last_processed, test_time)
    
    def test_bookmark_load_file_not_found(self):
        """Test bookmark load when file doesn't exist."""
        bookmark = Bookmark('BUSH_POINT', '/nonexistent/path/bookmark.json')
        bookmark.load()
        self.assertIsNone(bookmark.last_processed)


class TestProcessAudioDataMultiDay(unittest.TestCase):
    """Test the process_audio_data function with multi-day scenarios."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()
        self.bookmark_path = os.path.join(self.test_dir, 'test_bookmark.json')
        self.hydrophone = 'BUSH_POINT'
        
    def tearDown(self):
        """Clean up test files."""
        if os.path.exists(self.bookmark_path):
            os.remove(self.bookmark_path)
        os.rmdir(self.test_dir)
    
    @patch('git_action_batch.NoiseAnalysisPipeline')
    def test_two_consecutive_days(self, mock_pipeline_class):
        """Test processing spanning exactly two consecutive days."""
        # Setup
        mock_pipeline = Mock()
        mock_pipeline.generate_parquet_file.return_value = ('psd_path', 'broadband_path')
        mock_pipeline_class.return_value = mock_pipeline
        
        bookmark = Bookmark(self.hydrophone, self.bookmark_path)
        
        # Start at 10 PM on Jan 15, end at 2 AM on Jan 16
        pst = pytz.timezone('US/Pacific')
        start_time = pst.localize(dt.datetime(2026, 1, 15, 22, 0, 0))
        end_time = pst.localize(dt.datetime(2026, 1, 16, 2, 0, 0))
        
        # Execute
        process_audio_data(start_time, end_time, self.hydrophone, bookmark)
        
        # Verify pipeline was called twice (once per day)
        self.assertEqual(mock_pipeline_class.call_count, 2)
        self.assertEqual(mock_pipeline.generate_parquet_file.call_count, 2)
        
        # Check first call - should process from start_time to midnight
        first_call_args = mock_pipeline.generate_parquet_file.call_args_list[0][0]
        self.assertEqual(first_call_args[0], start_time)
        expected_midnight = pst.localize(dt.datetime(2026, 1, 16, 0, 0, 0))
        self.assertEqual(first_call_args[1], expected_midnight)
        
        # Check second call - should process from midnight to end_time
        second_call_args = mock_pipeline.generate_parquet_file.call_args_list[1][0]
        self.assertEqual(second_call_args[0], expected_midnight)
        self.assertEqual(second_call_args[1], end_time)
        
        # Verify correct folder paths were used
        first_folder_call = mock_pipeline_class.call_args_list[0][1]['pqt_folder']
        self.assertEqual(first_folder_call, 'data/hydrophone=BUSH_POINT/date=2026-01-15/')
        
        second_folder_call = mock_pipeline_class.call_args_list[1][1]['pqt_folder']
        self.assertEqual(second_folder_call, 'data/hydrophone=BUSH_POINT/date=2026-01-16/')
        
        # Verify bookmark was updated with final end_time
        self.assertEqual(bookmark.last_processed, end_time)
    
    @patch('git_action_batch.NoiseAnalysisPipeline')
    def test_three_consecutive_days(self, mock_pipeline_class):
        """Test processing spanning three consecutive days."""
        # Setup
        mock_pipeline = Mock()
        mock_pipeline.generate_parquet_file.return_value = ('psd_path', 'broadband_path')
        mock_pipeline_class.return_value = mock_pipeline
        
        bookmark = Bookmark(self.hydrophone, self.bookmark_path)
        
        # Start at 10 PM on Jan 15, end at 2 AM on Jan 17
        pst = pytz.timezone('US/Pacific')
        start_time = pst.localize(dt.datetime(2026, 1, 15, 22, 0, 0))
        end_time = pst.localize(dt.datetime(2026, 1, 17, 2, 0, 0))
        
        # Execute
        process_audio_data(start_time, end_time, self.hydrophone, bookmark)
        
        # Verify pipeline was called three times (once per day)
        self.assertEqual(mock_pipeline_class.call_count, 3)
        self.assertEqual(mock_pipeline.generate_parquet_file.call_count, 3)
        
        # Check time ranges for each call
        midnight_16 = pst.localize(dt.datetime(2026, 1, 16, 0, 0, 0))
        midnight_17 = pst.localize(dt.datetime(2026, 1, 17, 0, 0, 0))
        
        # First partition: Jan 15 22:00 to Jan 16 00:00
        first_call_args = mock_pipeline.generate_parquet_file.call_args_list[0][0]
        self.assertEqual(first_call_args[0], start_time)
        self.assertEqual(first_call_args[1], midnight_16)
        
        # Second partition: Jan 16 00:00 to Jan 17 00:00
        second_call_args = mock_pipeline.generate_parquet_file.call_args_list[1][0]
        self.assertEqual(second_call_args[0], midnight_16)
        self.assertEqual(second_call_args[1], midnight_17)
        
        # Third partition: Jan 17 00:00 to Jan 17 02:00
        third_call_args = mock_pipeline.generate_parquet_file.call_args_list[2][0]
        self.assertEqual(third_call_args[0], midnight_17)
        self.assertEqual(third_call_args[1], end_time)
        
        # Verify correct folder paths
        expected_folders = [
            'data/hydrophone=BUSH_POINT/date=2026-01-15/',
            'data/hydrophone=BUSH_POINT/date=2026-01-16/',
            'data/hydrophone=BUSH_POINT/date=2026-01-17/'
        ]
        
        for i, expected_folder in enumerate(expected_folders):
            actual_folder = mock_pipeline_class.call_args_list[i][1]['pqt_folder']
            self.assertEqual(actual_folder, expected_folder)
        
        # Verify bookmark was updated with final end_time
        self.assertEqual(bookmark.last_processed, end_time)
    
    @patch('git_action_batch.NoiseAnalysisPipeline')
    def test_bookmark_updates_after_each_partition(self, mock_pipeline_class):
        """Test that bookmark is updated after each day's processing."""
        # Setup
        mock_pipeline = Mock()
        mock_pipeline.generate_parquet_file.return_value = ('psd_path', 'broadband_path')
        mock_pipeline_class.return_value = mock_pipeline
        
        bookmark = Mock(spec=Bookmark)
        
        # Start at 10 PM on Jan 15, end at 2 AM on Jan 17 (3 days)
        pst = pytz.timezone('US/Pacific')
        start_time = pst.localize(dt.datetime(2026, 1, 15, 22, 0, 0))
        end_time = pst.localize(dt.datetime(2026, 1, 17, 2, 0, 0))
        
        # Execute
        process_audio_data(start_time, end_time, self.hydrophone, bookmark)
        
        # Verify bookmark.update was called 4 times (once per partition + final)
        # Actually it should be called 3 times during the loop, then once at the end
        # But the code calls it at the end of each iteration AND at the end of the function
        self.assertEqual(bookmark.update.call_count, 4)
        
        # Check the times passed to bookmark.update
        midnight_16 = pst.localize(dt.datetime(2026, 1, 16, 0, 0, 0))
        midnight_17 = pst.localize(dt.datetime(2026, 1, 17, 0, 0, 0))
        
        expected_updates = [midnight_16, midnight_17, end_time, end_time]
        actual_updates = [call[0][0] for call in bookmark.update.call_args_list]
        
        self.assertEqual(actual_updates, expected_updates)
    
    @patch('git_action_batch.NoiseAnalysisPipeline')
    def test_single_day_processing(self, mock_pipeline_class):
        """Test processing within a single day (else branch)."""
        # Setup
        mock_pipeline = Mock()
        mock_pipeline.generate_parquet_file.return_value = ('psd_path', 'broadband_path')
        mock_pipeline_class.return_value = mock_pipeline
        
        bookmark = Bookmark(self.hydrophone, self.bookmark_path)
        
        # Both times on same day
        pst = pytz.timezone('US/Pacific')
        start_time = pst.localize(dt.datetime(2026, 1, 15, 10, 0, 0))
        end_time = pst.localize(dt.datetime(2026, 1, 15, 14, 0, 0))
        
        # Execute
        process_audio_data(start_time, end_time, self.hydrophone, bookmark)
        
        # Verify pipeline was called only once
        self.assertEqual(mock_pipeline_class.call_count, 1)
        self.assertEqual(mock_pipeline.generate_parquet_file.call_count, 1)
        
        # Verify the time range
        call_args = mock_pipeline.generate_parquet_file.call_args[0]
        self.assertEqual(call_args[0], start_time)
        self.assertEqual(call_args[1], end_time)
        
        # Verify correct folder path
        folder_call = mock_pipeline_class.call_args[1]['pqt_folder']
        self.assertEqual(folder_call, 'data/hydrophone=BUSH_POINT/date=2026-01-15/')
        
        # Verify bookmark was updated
        self.assertEqual(bookmark.last_processed, end_time)
    
    @patch('git_action_batch.NoiseAnalysisPipeline')
    def test_pipeline_parameters(self, mock_pipeline_class):
        """Test that NoiseAnalysisPipeline is initialized with correct parameters."""
        # Setup
        mock_pipeline = Mock()
        mock_pipeline.generate_parquet_file.return_value = ('psd_path', 'broadband_path')
        mock_pipeline_class.return_value = mock_pipeline
        
        bookmark = Bookmark(self.hydrophone, self.bookmark_path)
        
        pst = pytz.timezone('US/Pacific')
        start_time = pst.localize(dt.datetime(2026, 1, 15, 22, 0, 0))
        end_time = pst.localize(dt.datetime(2026, 1, 16, 2, 0, 0))
        
        # Execute
        process_audio_data(start_time, end_time, self.hydrophone, bookmark)
        
        # Verify pipeline initialization parameters (for first call)
        from orcasound_noise.utils import Hydrophone
        first_init_call = mock_pipeline_class.call_args_list[0]
        
        # Check positional and keyword arguments
        self.assertEqual(first_init_call[0][0], Hydrophone.BUSH_POINT)
        self.assertEqual(first_init_call[1]['delta_f'], 10)
        self.assertIsNone(first_init_call[1]['bands'])
        self.assertEqual(first_init_call[1]['delta_t'], 60)
        self.assertEqual(first_init_call[1]['mode'], 'safe')
        self.assertEqual(first_init_call[1]['pqt_folder'], 'data/hydrophone=BUSH_POINT/date=2026-01-15/')
    
    @patch('git_action_batch.NoiseAnalysisPipeline')
    def test_generate_parquet_file_parameters(self, mock_pipeline_class):
        """Test that generate_parquet_file is called with correct parameters."""
        # Setup
        mock_pipeline = Mock()
        mock_pipeline.generate_parquet_file.return_value = ('psd_path', 'broadband_path')
        mock_pipeline_class.return_value = mock_pipeline
        
        bookmark = Bookmark(self.hydrophone, self.bookmark_path)
        
        pst = pytz.timezone('US/Pacific')
        start_time = pst.localize(dt.datetime(2026, 1, 15, 22, 0, 0))
        end_time = pst.localize(dt.datetime(2026, 1, 16, 2, 0, 0))
        
        # Execute
        process_audio_data(start_time, end_time, self.hydrophone, bookmark)
        
        # Check that upload_to_s3=False for all calls
        for call_obj in mock_pipeline.generate_parquet_file.call_args_list:
            self.assertEqual(call_obj[1]['upload_to_s3'], False)
    
    @patch('git_action_batch.NoiseAnalysisPipeline')
    def test_month_boundary_crossing(self, mock_pipeline_class):
        """Test processing that crosses month boundaries."""
        # Setup
        mock_pipeline = Mock()
        mock_pipeline.generate_parquet_file.return_value = ('psd_path', 'broadband_path')
        mock_pipeline_class.return_value = mock_pipeline
        
        bookmark = Bookmark(self.hydrophone, self.bookmark_path)
        
        # Start at Jan 31 10 PM, end at Feb 1 2 AM
        pst = pytz.timezone('US/Pacific')
        start_time = pst.localize(dt.datetime(2026, 1, 31, 22, 0, 0))
        end_time = pst.localize(dt.datetime(2026, 2, 1, 2, 0, 0))
        
        # Execute
        process_audio_data(start_time, end_time, self.hydrophone, bookmark)
        
        # Verify correct folder paths across month boundary
        expected_folders = [
            'data/hydrophone=BUSH_POINT/date=2026-01-31/',
            'data/hydrophone=BUSH_POINT/date=2026-02-01/'
        ]
        
        for i, expected_folder in enumerate(expected_folders):
            actual_folder = mock_pipeline_class.call_args_list[i][1]['pqt_folder']
            self.assertEqual(actual_folder, expected_folder)


if __name__ == '__main__':
    unittest.main()
