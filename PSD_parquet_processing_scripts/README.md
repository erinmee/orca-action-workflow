# Github Actions automated pipeline for sound file to Power Spectral Density (PSD) parquet files

## Purpose

The purpose of this github actions workflow is to asyncronously automate the processing of .ts sound files into a PSD parquet dataframe and publish the parquet files to an S3 bucket. The store of a PSD dataframe facilitates retrospective analyses of sound pollution and Orca call signals and their relationships.

## Script/Workflow Features

* Partitioning of parquet file by Hydrophone and date. 
    * ex. data/hydrophone=BUSH_POINT/data=2026-01-16/...
* Bookmarking using a json file uploaded to S3
* File compression of parquet files into single file after one day of records
* CI testing to design for file handling errors

## Script process
```mermaid
flowchart TB;
    n3["bookmark available?"] -- yes --> n4["start_time = load bookmark"];
    n3 -- no --> n5["start_time = create bookmark for 1 hr earlier"]
    n4 --> n6["end_time = now"]
    n5 --> n6
    n6 --> n7["start_time and end_time on same date"] & n10["start_time and end_time span different dates"]
    n7 --> n8["partition: date1/..."]
    n8 --> n9["NoiseAnalysisPipeline for start_time to end_time"]
    n10 --> n11["partion: date1/..."] & n12["partition: date2/..."] & n13["partition: date3/..."]
    n11 --> n14["NoiseAnalysisPipeline for <br>start_time to date2:00:00:00"]
    n12 --> n15@{ label: "NoiseAnalysisPipeline for<br style=\"--tw-scale-x:\">date2:00:00:00 to date3:00:00:00" }
    n13 --> n16@{ label: "NoiseAnalysisPipeline for<br style=\"--tw-scale-x:\">date3:00:00:00 to end_time" }
    n9 --> n17["bookmark = end_time"]
    n14 --> n17
    n15 --> n17
    n16 --> n17
    n18["For every hydrophone"] --> n3

    n3@{ shape: rounded}
    n4@{ shape: rounded}
    n5@{ shape: rounded}
    n6@{ shape: rounded}
    n7@{ shape: rounded}
    n10@{ shape: rounded}
    n8@{ shape: rounded}
    n9@{ shape: rounded}
    n11@{ shape: rounded}
    n12@{ shape: rounded}
    n13@{ shape: rounded}
    n14@{ shape: rounded}
    n15@{ shape: rounded}
    n16@{ shape: rounded}
    n17@{ shape: rounded}
    n18@{ shape: rounded}
```

## Development TODOs

- ~~Workflow able to save parquet files to repo~~ - 1/14/26
- ~~Bookmarking tentitively working~~ - 1/16/26
- ~~Add functionality to handle two day spans, i.e. time period being processed spans midnight~~ 1/19/26
- ~~Temporary fix for orca-hls-utils errors, using developmental version of orca-hls-utils~~ 1/19/26
- Add metadata generation? (.ts files processed, corresponding time, partial/complete processing of file, file issues)
- Add file compression workflow?
- ~~Add matrix jobs for all hydrophones~~ 1/19/26
- Add publishing to S3