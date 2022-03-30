# Vizarre

## Summary

[Workout Wednesday](https://www.workout-wednesday.com/power-bi-challenges/)'s challenges.

### How to visualise the dashboards

Unfortunately, I do not own a PowerBI Premium account, so you need download the .pbix files and execute then in PowerBI desktop.

### Pipeline

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
