from google.cloud.bigquery import LoadJobConfig, SchemaField, SourceFormat


class BigQueryHelper:
    def __init__(self, BUCKET, DATASET, bq_client):
        self.BUCKET = BUCKET
        self.DATASET = DATASET
        self.bq_client = bq_client

    def load_job_cb(future):
        """Prints update upon completion to output of last run cell."""
        
        seconds = (future.ended - future.created).total_seconds()
        print(f'Loaded {future.output_rows:,d} rows to table {future.job_id.split("_")[0]} in '
            f'{seconds:>4,.1f} sec, {int(future.output_rows / seconds):,d} per sec.')
        
    def load_csv_uri(self, table_id, schemas_orig):
        full_table_id = f'{self.DATASET}.{table_id}'

        job_config = LoadJobConfig(
            schema=schemas_orig[table_id],
            source_format=SourceFormat.CSV,
            skip_leading_rows=1, autodetect=schemas[table_id] is None)

        uri = f'gs://{self.BUCKET}/{table_id}.csv'
        load_job = self.bq_client.load_table_from_uri(uri, full_table_id,
                                                job_config=job_config,
                                                job_id_prefix=f'{table_id}_')
        print(f'job {load_job.job_id} started')
        load_job.add_done_callback(load_job_cb)
        
        return load_job
        
    def load_json_file(self, table_id, schemas_orig):
        full_table_id = f'{self.DATASET}.{table_id}'

        job_config = LoadJobConfig(
            schema=schemas_orig[table_id],
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON)

        file_path = f'{table_id}.json'
        with open(file_path, "rb") as source_file:
            load_job = self.bq_client.load_table_from_file(source_file,
                                                    full_table_id,
                                                    job_config=job_config,
                                                    job_id_prefix=f'{table_id}_')
        print(f'job {load_job.job_id} started')
        load_job.add_done_callback(load_job_cb)
        
        return load_job

    def get_table(self, table_id):
        return self.bq_client.get_table(f'{self.DATASET}.{table_id}')

    def del_table(self, table_id):
        return self.bq_client.delete_table(f'{self.DATASET}.{table_id}',
                                    not_found_ok=True)