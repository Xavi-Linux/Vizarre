# Vizarre

## Summary

I am using this repository to exhibit my solutions for [Workout Wednesday](https://www.workout-wednesday.com/power-bi-challenges/)'s PowerBI challenges. For every solved challenge, you will find a folder holding my answer and the challenge's requirements.

Additionally, I am taking advantage of those challenges to improve my BigQuery and Google Data Studio skills. So I have also designed some dashboards in Google Data Studio with some of the available datasets. To feed Google DataStudio, I have previously uploaded the datasets to BigQuery.

### How to visualise the PowerBI dashboards

Unfortunately, I do not own a PowerBI Premium account, so you need to download the .pbix files and execute them in PowerBI desktop.

### Google DataStudio dashboards

|Dataset|Link|
|:------|:---:|
| ncaa  | [see](https://datastudio.google.com/s/rIuDv6tIC1U)|

### Pipeline design

This is the suggested ETL pipeline:

![Pipeline](/img/Pipeline.JPG)

### Technical requirements

If you want to run a whole pipeline, you need:

1. A UNIX operating system

2. The Google Cloud SDK

3. A GCP project with both Storage and BigQuery apis enabled

4. Make sure you have all Python packages listed in [requirements.txt](requirements.txt) installed.

5. Replace XXXX values in [dummy.keys](dummy.keys) file with the requested information (GCP project id, GCP project location & an existing GCP bucket.)

6. Replace value of keys_file variable in the [pipeline script](bin/execute_pipeline.sh) with dummy.keys.

### How to run a pipeline

1. Save the target dataset in [unprocessed folder](datasets/unprocessed/)
2. Execute:

```shell
./bin/execute_pipeline.sh -f path/to/target/dataset -n name_of_the_pipeline
```

The name of the pipeline can be anything you want.

#### Other aspects

1. If something has gone wrong while executing the pipeline, you can run a rollback:

```shell
./bin/rollback_pipeline.sh -n name_of_the_pipeline
```

2. If your pipeline does not fit an **Excel-to-csv** transformation, you can easily adapt the pipeline to your needs by subclassing the BasePipeline in [pipelines.py](bin/vizproc/pipelines.py). Make sure the NAME attribute name is changed to the intended value for your pipeline when running the bash script.

3. Make sure that your pipelines generate a json file for each table. They help infer the schemas for Bigquery tables.
