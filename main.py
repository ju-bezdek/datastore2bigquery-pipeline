import argparse
import os
import datetime
import logging
from typing import Dict
import apache_beam as beam
import yaml
from apache_beam import TaggedOutput
from apache_beam.io.gcp.bigquery_file_loads import BigQueryBatchFileLoads
from apache_beam.io.gcp.datastore.v1new.datastoreio import ReadFromDatastore
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions

from transform.datastore import entity_to_json,  CreateEntitiesLoadQuery, get_entity_filters, QueryFn



from apache_beam.io.gcp.datastore.v1new.types import Query, Entity


class CustomPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--conf',
            dest='conf',
            #required=True,
            default='config.yaml'
        )

        parser.add_argument(
            '--since_time',
            dest='since_time',
            #required=True,
            default = "H-48"
        )


def run(argv=None):
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./secrets/credentials.json"
    """Main entry point to the pipeline."""

    pipeline_options = CustomPipelineOptions(argv)
    
    
    conf = yaml.load(open(pipeline_options.conf, 'r'), Loader=yaml.SafeLoader)
    
    if pipeline_options.since_time.startswith("D-"):
        since_time = datetime.datetime.now() - datetime.timedelta(days=int(pipeline_options.since_time[2:]))
    elif pipeline_options.since_time.startswith("H-"):
        since_time = datetime.datetime.now() - datetime.timedelta(hours=int(pipeline_options.since_time[2:]))
    else:
        since_time = datetime.datetime.fromisoformat(pipeline_options.since_time)
    entity_filtering = get_entity_filters(conf['kinds_to_export'],since_time)
    
    run_options = conf['run_options']
    output_dataset = run_options["dataset"]

    project_id = run_options["project_id"]
    pipeline_options.view_as(GoogleCloudOptions).project = project_id

    pipeline_options.view_as(beam.options.pipeline_options.SetupOptions).setup_file = './setup.py'
    pipeline_options.view_as(beam.options.pipeline_options.SetupOptions).save_main_session = True
    pipeline_options.view_as(beam.options.pipeline_options.GoogleCloudOptions).region = run_options["region"] # "us-east1"
    pipeline_options.view_as(beam.options.pipeline_options.WorkerOptions).machine_type = run_options["machine_type"] # 'n1-standard-1'
    pipeline_options.view_as(beam.options.pipeline_options.WorkerOptions).num_workers = run_options["num_workers"] # 2
    pipeline_options.view_as(beam.options.pipeline_options.WorkerOptions).disk_size_gb = run_options["disk_size_gb"] # 25
    pipeline_options.view_as(beam.options.pipeline_options.WorkerOptions).autoscaling_algorithm = run_options["autoscaling_algorithm"]

    path_prefix = run_options["gcs_temp_dir"]
    gcs_temp_dir = f'{path_prefix}/{datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")}'

    collections_to_process = [kind for kind in conf['kinds_to_export']]
    
    with beam.Pipeline(options=pipeline_options) as p:
        # Create a query and filter
        extracted_data = (p
                | 'define collections' >> beam.Create(collections_to_process)
                | 'create queries' >> beam.ParDo(CreateEntitiesLoadQuery(project_id, entity_filtering))
                | 'read from datastore' #>> ReadFromDatastore(Query(kind="channels", project=project_id))
                    >> beam.ParDo(QueryFn())
        )
            
        transformed_data = ( extracted_data 
                | 'convert entities' >> beam.Map(entity_to_json)
                )

        load =  (transformed_data
                | 'write merge' >> BigQueryBatchFileLoads(
                    destination=lambda row: f"{project_id}:{output_dataset}.{row['_kind'].lower()}",
                    custom_gcs_temp_location=f'{path_prefix}', #/truncate',
                    write_disposition='WRITE_TRUNCATE', # write truncate merge data in target
                    create_disposition='CREATE_IF_NEEDED', #create table if not extis
                    schema='SCHEMA_AUTODETECT') # autgenerate table schema by data provided
                )
        

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()