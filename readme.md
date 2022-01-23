# Datapipeline to extract newer data from Google Datastore to BigQuery

Extracts newer data for defined kinds from Google datastore, filtered by some timestamp column

## Usage

configure config.yaml as by provideded config_example.yaml

download your google cloud service account key to ./secrets/credentials.json

For local run

```sh
python main.py --runner direct 
```

For run as Google Dataflow pipeline

```sh
python main.py --runner dataflow
```

### References

Inspired by similar project [jkrajniak/demo-datastore-export-filtering](https://github.com/jkrajniak/demo-datastore-export-filtering)