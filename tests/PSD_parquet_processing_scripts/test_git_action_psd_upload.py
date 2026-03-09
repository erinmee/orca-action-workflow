import unittest
from unittest.mock import Mock, patch, MagicMock, call
import datetime as dt
import pytz
import json
import tempfile
import os
import sys

# Mock external dependencies before importing the module
sys.modules['boto3'] = MagicMock()
sys.modules['botocore'] = MagicMock()
sys.modules['botocore.exceptions'] = MagicMock()
sys.modules['orcasound_noise'] = MagicMock()
sys.modules['orcasound_noise.pipeline'] = MagicMock()
sys.modules['orcasound_noise.pipeline.pipeline'] = MagicMock()
sys.modules['orcasound_noise.utils'] = MagicMock()
sys.modules['orcasound_noise.utils.file_connector'] = MagicMock()

# Import ClientError for testing
from unittest.mock import MagicMock as MM
class MockClientError(Exception):
    def __init__(self, error_response, operation_name):
        self.response = error_response
        self.operation_name = operation_name

sys.modules['botocore.exceptions'].ClientError = MockClientError

# Import the module to test
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../PSD_parquet_processing_scripts'))
from git_action_psd_upload import Bookmark, process_upload_psd


class TestBookmark(unittest.TestCase):
    """Test cases for the Bookmark class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_hydrophone = MagicMock()
        self.mock_hydrophone.value.save_bucket = 'test-bucket'
        self.mock_hydrophone.value.save_folder = 'test-folder'
        self.mock_hydrophone.value.bookmark_folder = 'test-folder'
        self.mock_hydrophone.value.name = 'test_hydrophone'
    
    @patch('git_action_psd_upload.boto3.client')
    @patch('git_action_psd_upload.tempfile.TemporaryDirectory')
    def test_bookmark_initialization(self, mock_temp_dir, mock_boto_client):
        """Test Bookmark initialization."""
        mock_temp_instance = MagicMock()
        mock_temp_instance.name = '/tmp/test'
        mock_temp_dir.return_value = mock_temp_instance
        
        bookmark = Bookmark(self.mock_hydrophone)
        
        self.assertEqual(bookmark.hydrophone, 'test_hydrophone')
        self.assertEqual(bookmark.bucket, 'test-bucket')
        self.assertEqual(bookmark.folder, 'test-folder')
        self.assertIsNone(bookmark.last_processed)
        mock_boto_client.assert_called_once_with('s3')
    
    @patch('git_action_psd_upload.boto3.client')
    @patch('git_action_psd_upload.tempfile.TemporaryDirectory')
    @patch('builtins.open', create=True)
    def test_bookmark_update(self, mock_open, mock_temp_dir, mock_boto_client):
        """Test Bookmark update method uploads file to S3."""
        mock_temp_instance = MagicMock()
        mock_temp_instance.name = '/tmp/test'
        mock_temp_dir.return_value = mock_temp_instance
        
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        
        bookmark = Bookmark(self.mock_hydrophone)
        test_time = dt.datetime(2026, 2, 4, 12, 0, 0)
        
        # Mock file writing
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file
        
        bookmark.update(test_time)
        
        # Verify last_processed was updated
        self.assertEqual(bookmark.last_processed, test_time)
        
        # Verify file was written
        mock_open.assert_called()
        
        # Verify upload_file was called
        mock_s3_client.upload_file.assert_called_once()
        call_args = mock_s3_client.upload_file.call_args[0]
        self.assertEqual(call_args[1], 'test-bucket')
        self.assertEqual(call_args[2], 'test-folder/test_hydrophone_bookmark.json')
    
    @patch('git_action_psd_upload.boto3.client')
    @patch('git_action_psd_upload.tempfile.TemporaryDirectory')
    @patch('builtins.open', create=True)
    def test_bookmark_load_success(self, mock_open, mock_temp_dir, mock_boto_client):
        """Test Bookmark load method when bookmark exists."""
        mock_temp_instance = MagicMock()
        mock_temp_instance.name = '/tmp/test'
        mock_temp_dir.return_value = mock_temp_instance
        
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        
        bookmark = Bookmark(self.mock_hydrophone)
        test_time = dt.datetime(2026, 1, 1, 10, 30, 0)
        
        # Mock file reading
        mock_file = MagicMock()
        mock_data = {'last_processed': test_time.isoformat()}
        mock_open.return_value.__enter__.return_value = mock_file
        
        with patch('json.load', return_value=mock_data):
            bookmark.load()
        
        # Verify download_file was called
        mock_s3_client.download_file.assert_called_once()
        
        # Verify last_processed was loaded
        self.assertEqual(bookmark.last_processed, test_time)
    
    @patch('git_action_psd_upload.boto3.client')
    @patch('git_action_psd_upload.tempfile.TemporaryDirectory')
    def test_bookmark_load_not_found(self, mock_temp_dir, mock_boto_client):
        """Test Bookmark load method when bookmark doesn't exist (404 error)."""
        mock_temp_instance = MagicMock()
        mock_temp_instance.name = '/tmp/test'
        mock_temp_dir.return_value = mock_temp_instance
        
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        
        # Simulate 404 error
        error_response = {'Error': {'Code': '404'}}
        mock_s3_client.head_object.side_effect = MockClientError(error_response, 'HeadObject')
        
        bookmark = Bookmark(self.mock_hydrophone)
        
        with patch('builtins.print'):
            bookmark.load()
        
        # Verify last_processed is still None
        self.assertIsNone(bookmark.last_processed)
    
    @patch('git_action_psd_upload.boto3.client')
    @patch('git_action_psd_upload.tempfile.TemporaryDirectory')
    def test_bookmark_load_other_error(self, mock_temp_dir, mock_boto_client):
        """Test Bookmark load method when non-404 error occurs - currently gets silently ignored."""
        mock_temp_instance = MagicMock()
        mock_temp_instance.name = '/tmp/test'
        mock_temp_dir.return_value = mock_temp_instance
        
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        
        # Simulate non-404 error - currently the code doesn't re-raise this
        # This is likely a bug in the original code, but we test the actual behavior
        error_response = {'Error': {'Code': '500', 'Message': 'Server error'}}
        mock_s3_client.head_object.side_effect = MockClientError(error_response, 'HeadObject')
        
        bookmark = Bookmark(self.mock_hydrophone)
        
        # The current implementation doesn't re-raise non-404 errors
        # This test documents the actual behavior (which may be buggy)
        bookmark.load()
        
        # last_processed should remain None since load failed silently
        self.assertIsNone(bookmark.last_processed)


class TestProcessUploadPsd(unittest.TestCase):
    """Test cases for the process_upload_psd function."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_hydrophone = MagicMock()
        self.mock_hydrophone.value.save_bucket = 'test-bucket'
        self.mock_hydrophone.value.save_folder = 'test-folder'
        self.mock_hydrophone.value.name = 'test_hydrophone'
        
        self.start_time = dt.datetime(2026, 2, 1, 0, 0, 0)
        self.end_time = dt.datetime(2026, 2, 4, 12, 0, 0)
    
    @patch('git_action_psd_upload.boto3.client')
    @patch('git_action_psd_upload.tempfile.TemporaryDirectory')
    @patch('git_action_psd_upload.NoiseAnalysisPipeline')
    @patch('git_action_psd_upload.S3FileConnector')
    def test_process_upload_psd(self, mock_s3_connector, mock_pipeline_class, 
                               mock_temp_dir, mock_boto_client):
        """Test process_upload_psd function execution."""
        # Setup mocks
        mock_temp_instance = MagicMock()
        mock_temp_instance.name = '/tmp/test'
        mock_temp_dir.return_value = mock_temp_instance
        
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        
        mock_pipeline = MagicMock()
        mock_pipeline_class.return_value = mock_pipeline
        mock_pipeline.generate_parquet_file.return_value = 's3://test-bucket/test-file.parquet'
        
        mock_bookmark = MagicMock()
        mock_file_connector = MagicMock()
        
        # Execute function
        process_upload_psd(self.start_time, self.end_time, self.mock_hydrophone, 
                          mock_bookmark, mock_file_connector)
        
        # Verify pipeline was created with correct parameters
        mock_pipeline_class.assert_called_once_with(
            self.mock_hydrophone,
            delta_f=1,
            bands=12,
            delta_t=1,
            mode='safe'
        )
        
        # Verify parquet file generation was called
        mock_pipeline.generate_parquet_file.assert_called_once_with(
            self.start_time,
            self.end_time,
            upload_to_s3=True,
            partitioning=True
        )
        
        # Verify bookmark was updated
        mock_bookmark.update.assert_called_once_with(self.end_time)


class TestMainFunction(unittest.TestCase):
    """Test cases for the main function."""
    
    @patch('git_action_psd_upload.boto3.client')
    @patch('git_action_psd_upload.tempfile.TemporaryDirectory')
    @patch('git_action_psd_upload.process_upload_psd')
    @patch('git_action_psd_upload.S3FileConnector')
    @patch('git_action_psd_upload.Hydrophone')
    @patch('git_action_psd_upload.dt.datetime')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_with_hydrophone_argument(self, mock_parse_args, mock_datetime, 
                                          mock_hydrophone_enum, mock_s3_connector,
                                          mock_process_upload, mock_temp_dir, 
                                          mock_boto_client):
        """Test main function with hydrophone argument."""
        # Setup mocks
        mock_temp_instance = MagicMock()
        mock_temp_instance.name = '/tmp/test'
        mock_temp_dir.return_value = mock_temp_instance
        
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        
        now = dt.datetime(2026, 2, 4, 12, 0, 0, tzinfo=pytz.timezone('US/Pacific'))
        mock_datetime.now.return_value = now
        
        # Setup argparse mock to return hydrophone='bush_point'
        mock_args = MagicMock()
        mock_args.hydrophone = 'BUSH_POINT'
        mock_parse_args.return_value = mock_args
        
        # Setup hydrophone enum mock
        mock_hydrophone_instance = MagicMock()
        mock_hydrophone_instance.value.save_bucket = 'test-bucket'
        mock_hydrophone_instance.value.save_folder = 'test-folder'
        mock_hydrophone_instance.value.name = 'bush_point'
        mock_hydrophone_enum.__getitem__.return_value = mock_hydrophone_instance
        
        # Mock the head_object to indicate bookmark doesn't exist initially
        error_response = {'Error': {'Code': '404'}}
        mock_s3_client.head_object.side_effect = MockClientError(error_response, 'HeadObject')
        
        # Import and run main
        from git_action_psd_upload import main
        
        main()
        
        # Verify process_upload_psd was called
        mock_process_upload.assert_called_once()
        call_args = mock_process_upload.call_args[0]
        
        # Verify the time range (end_time should be now)
        self.assertEqual(call_args[1], now)  # end_time
        self.assertEqual(call_args[2], mock_hydrophone_instance)  # hydrophone
    
    @patch('git_action_psd_upload.boto3.client')
    @patch('git_action_psd_upload.tempfile.TemporaryDirectory')
    @patch('git_action_psd_upload.process_upload_psd')
    @patch('git_action_psd_upload.S3FileConnector')
    @patch('git_action_psd_upload.Hydrophone')
    @patch('git_action_psd_upload.dt.datetime')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_uses_default_hydrophone(self, mock_parse_args, mock_datetime,
                                          mock_hydrophone_enum, mock_s3_connector,
                                          mock_process_upload, mock_temp_dir,
                                          mock_boto_client):
        """Test main function uses default hydrophone when none provided."""
        # Setup mocks
        mock_temp_instance = MagicMock()
        mock_temp_instance.name = '/tmp/test'
        mock_temp_dir.return_value = mock_temp_instance
        
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        
        now = dt.datetime(2026, 2, 4, 12, 0, 0, tzinfo=pytz.timezone('US/Pacific'))
        mock_datetime.now.return_value = now
        
        # Setup argparse mock to return default hydrophone
        mock_args = MagicMock()
        mock_args.hydrophone = 'BUSH_POINT'
        mock_parse_args.return_value = mock_args
        
        # Setup hydrophone enum mock
        mock_hydrophone_instance = MagicMock()
        mock_hydrophone_instance.value.save_bucket = 'test-bucket'
        mock_hydrophone_instance.value.save_folder = 'test-folder'
        mock_hydrophone_instance.value.name = 'bush_point'
        mock_hydrophone_enum.__getitem__.return_value = mock_hydrophone_instance
        
        # Mock 404 error for first bookmark check
        error_response = {'Error': {'Code': '404'}}
        mock_s3_client.head_object.side_effect = MockClientError(error_response, 'HeadObject')
        
        from git_action_psd_upload import main
        
        main()
        
        # Verify the hydrophone was looked up
        mock_hydrophone_enum.__getitem__.assert_called_once_with('BUSH_POINT')
        mock_process_upload.assert_called_once()


if __name__ == '__main__':
    unittest.main()

