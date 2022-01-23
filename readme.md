# Datapipeline to extract newer data from datastore to BigQuery

Extracts newer data for defined kinds from Google datastore, filtered by some timestamp column

## Usage

configure config.yaml as by provideded config_example.yaml

download your google cloud service account key to ./secrets/credentials.json

For local run
> python main.py --runner DirectRunner 

For run as Google DataFlow pipeline
> python main.py --runner DirectRunner 