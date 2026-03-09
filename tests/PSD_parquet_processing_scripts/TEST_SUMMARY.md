# Unit Tests for git_action_psd_upload.py

## Overview
This test suite provides comprehensive coverage for the `git_action_psd_upload.py` script, which handles PSD (Power Spectral Density) parquet file generation and uploads for hydrophone audio data.

## Test Classes and Coverage

### TestBookmark (5 tests)
Tests for the `Bookmark` class which manages hydrophone processing checkpoints in S3.

1. **test_bookmark_initialization**
   - Verifies proper initialization of Bookmark instance with hydrophone metadata
   - Checks S3 bucket/folder configuration
   - Validates boto3 S3 client creation

2. **test_bookmark_update**
   - Tests updating bookmark with new timestamp
   - Verifies JSON file creation with ISO format datetime
   - Confirms S3 upload to correct bucket/key location

3. **test_bookmark_load_success**
   - Tests loading existing bookmark from S3
   - Verifies S3 download functionality
   - Confirms datetime parsing from stored JSON

4. **test_bookmark_load_not_found**
   - Tests handling when bookmark doesn't exist (404 error)
   - Verifies graceful handling with no exception
   - Confirms `last_processed` remains None

5. **test_bookmark_load_other_error**
   - Tests behavior when non-404 errors occur during load
   - Documents current behavior (errors are silently ignored)
   - Note: This may indicate a potential bug in error handling

### TestProcessUploadPsd (1 test)
Tests for the `process_upload_psd` function which orchestrates PSD file generation.

1. **test_process_upload_psd**
   - Verifies NoiseAnalysisPipeline creation with correct parameters
   - Confirms parquet file generation with proper time range
   - Validates bookmark update is called after processing

### TestMainFunction (2 tests)
Tests for the `main` function which serves as the CLI entry point.

1. **test_main_with_hydrophone_argument**
   - Tests main function when hydrophone argument is provided
   - Verifies correct hydrophone enum lookup
   - Confirms process_upload_psd is called with correct parameters

2. **test_main_uses_default_hydrophone**
   - Tests main function with default hydrophone (BUSH_POINT)
   - Validates proper timezone handling (US/Pacific)
   - Confirms time range calculation when no bookmark exists

## Test Execution

Run all tests:
```bash
python -m pytest tests/PSD_parquet_processing_scripts/test_git_action_psd_upload.py -v
```

Run specific test class:
```bash
python -m pytest tests/PSD_parquet_processing_scripts/test_git_action_psd_upload.py::TestBookmark -v
```

Run specific test:
```bash
python -m pytest tests/PSD_parquet_processing_scripts/test_git_action_psd_upload.py::TestBookmark::test_bookmark_update -v
```

## Mocking Strategy

- **External Dependencies**: boto3, orcasound_noise, and botocore are mocked to avoid environment dependencies
- **S3 Operations**: Mocked boto3 S3 client for upload/download operations
- **File I/O**: Mocked file operations and temporary directories
- **Pipeline**: Mocked NoiseAnalysisPipeline to isolate function testing
- **CLI Parsing**: Mocked argparse to control command-line arguments

## Known Issues/Notes

1. The `test_bookmark_load_other_error` test documents a potential bug where non-404 ClientErrors are silently ignored rather than re-raised.
2. Tests use mocked S3 operations - integration tests with real S3 would require separate test fixtures.
3. Datetime mocking focuses on timezone handling for US/Pacific timezone as per the script.

## Test Results
✅ All 8 tests pass successfully
