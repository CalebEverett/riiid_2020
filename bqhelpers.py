from pathlib import Path
import pytz
import sys

from google.cloud.bigquery import ExtractJobConfig, LoadJobConfig, \
    SchemaField, SourceFormat
import pandas as pd
from tqdm.notebook import tqdm


class BQHelper:
    def __init__(self, bucket, DATASET, bq_client):
        self.bucket = bucket
        self.BUCKET = self.bucket.name
        self.DATASET = DATASET
        self.bq_client = bq_client
   
    # LOAD FUNCTTIONS 
    # ================
    def load_job_cb(self, future):
        """Prints update upon completion to output of last run cell."""
        
        seconds = (future.ended - future.created).total_seconds()
        print(f'Loaded {future.output_rows:,d} rows to table {future.job_id.split("_")[0]} in '
            f'{seconds:>4,.1f} sec, {int(future.output_rows / seconds):,d} per sec.')
        
    def load_csv_uri(self, table_id, schemas_orig):
        full_table_id = f'{self.DATASET}.{table_id}'

        job_config = LoadJobConfig(
            schema=schemas_orig[table_id],
            source_format=SourceFormat.CSV,
            skip_leading_rows=1
            )

        uri = f'gs://{self.BUCKET}/{table_id}.csv'
        load_job = self.bq_client.load_table_from_uri(uri, full_table_id,
                                                job_config=job_config,
                                                job_id_prefix=f'{table_id}_')
        print(f'job {load_job.job_id} started')
        load_job.add_done_callback(self.load_job_cb)
        
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
        load_job.add_done_callback(self.load_job_cb)
        
        return load_job

    def get_table(self, table_id):
        return self.bq_client.get_table(f'{self.DATASET}.{table_id}')

    def del_table(self, table_id):
        return self.bq_client.delete_table(f'{self.DATASET}.{table_id}',
                                    not_found_ok=True)

    def get_df_jobs(self, max_results=10):
        jobs = self.bq_client.list_jobs(max_results=max_results, all_users=True)
        jobs_list = []

        if jobs.num_results:
            for job in jobs:
                ended = job.ended if job.ended else datetime.now(pytz.UTC)
                exception = job.exception() if job.ended else None
                jobs_list.append({'job_id': job.job_id, 'job_type': job.job_type,
                            'started': job.started, 'ended': ended,
                            'running': job.running(),
                            'exception': exception,
                            })
            df_jobs = pd.DataFrame(jobs_list)
            df_jobs['seconds'] = (df_jobs.ended - df_jobs.started).dt.seconds
            df_jobs.started = df_jobs.started.astype(str).str[:16]
            del df_jobs['ended']
            return df_jobs
        else:
            return None

    def get_df_table_list(self):
        tables = []
        for t in self.bq_client.list_tables(self.DATASET):
            table = self.bq_client.get_table(t)
            tables.append({'table_id': table.table_id, 'cols': len(table.schema),
                        'rows': table.num_rows, 'kb': int(table.num_bytes/1e3)})
        df_tables = pd.DataFrame(tables)
        
        return df_tables

    # QUERY FUNCTIONS
    # ================
    def done_cb(self, future):
        seconds = (future.ended - future.started).total_seconds()
        print(f'Job {future.job_id} finished in {seconds} seconds.')

    def run_query(self, query, job_id_prefix=None, wait=False):
        query_job = self.bq_client.query(query, job_id_prefix=job_id_prefix)
        print(f'Job {query_job.job_id} started.')
        query_job.add_done_callback(self.done_cb)
        if wait:
            query_job.result()
        
        return query_job

    def get_df_query(self, query, dtypes=None):
        query_job = self.run_query(*query)

        df_query = query_job.to_dataframe(dtypes=dtypes, 
                                progress_bar_type='tqdm_notebook')
        return df_query

    def get_df_table(self, table_id, max_results=10000, dtypes=None):
        table = self.get_table(table_id)
        df_table = (self.bq_client.list_rows(table, max_results=max_results)
                    .to_dataframe(dtypes=dtypes,
                                progress_bar_type='tqdm_notebook'))
        return df_table

    # EXPORT FUNCTIONS
    # ================
    def export_query_gcs(self, query, file_format='csv', wait=True):
        """ Uses BigQuery temporary table reference as gcs prefix.
        Runs query and exports to gcs if it doesn't already exist in gcs.
        Exported in multiple files if over 1GB. Returns gcs prefix.
        """
        qj = self.run_query(*query, wait=wait)
        
        prefix = ('/').join(qj.destination.path.split('/')[-2:])
        blobs_list = list(self.bucket.list_blobs(prefix=prefix))
        
        if not blobs_list:
            
            job_prefix_id = sys._getframe().f_code.co_name + '_'
            
            formats={'csv': 'CSV',
                    'json': 'NEWLINE_DELIMITED_JSON'}
            
            job_config = ExtractJobConfig(destination_format=formats[file_format])
            
            ex_job = self.bq_client.extract_table(
                source=qj.destination,
                destination_uris=f'gs://{self.BUCKET}/{prefix}/*.{file_format}',
                job_id_prefix=job_prefix_id,
                job_config=job_config)
        
            ex_job.add_done_callback(self.done_cb)
            
            print(f'Job {ex_job.job_id} started.')
        
            if wait:
                ex_job.result()
                blobs_list = list(self.bucket.list_blobs(prefix=prefix))
                n_files = len(blobs_list) 
                print(f'{n_files} file{"s" if n_files > 1 else ""} '
                    f'exported to gcs with prefix {prefix}.')
        
        else:
            n_files = len(blobs_list) 
            print(f'{n_files} file{"s" if n_files > 1 else ""} '
                  f'already exist{"s" if n_files == 1 else ""} in gcs with '
                  f'prefix {prefix}.')
        
        return prefix

    def get_table_gcs(self, prefix):
        """Downloads all files at prefix if they don't exist locally.
        Returns list of file paths.
        """
        
        file_paths = list(Path().glob(prefix))
        if not file_paths:
            blobs_list = list(self.bucket.list_blobs(prefix=prefix))
            n_files = len(blobs_list)
            print(f'Downloading {n_files} file{"s" if n_files > 1 else ""} '
                  f'from gcs for table {prefix}...')

            for b in tqdm(blobs_list, desc='Files Downloaded: '):
                print('Downloading', b.name, b.size)
                Path(b.name).parent.mkdir(parents=True, exist_ok=True)
                b.download_to_filename(b.name)
        
        else:
            n_files = len(list(file_paths[0].iterdir()))
            print(f'{n_files} file{"s" if n_files > 1 else ""} already '
                  f'exist{"s" if n_files == 1 else ""} locally for '
                  f'table{prefix}.')
            
        return list(list(Path().glob(prefix))[0].iterdir())

    def get_df_files(self, file_paths, dtypes):
        """ Creates data frame from list of local file paths.
        Returns dataframe.
        """
        
        prefix = str(file_paths[0].parent)
        suffix = file_paths[0].suffix
        
        n_files = len(file_paths)
        print(f'Creating dataframe from {n_files} file{"s" if n_files > 1 else ""} for table {prefix}...')
        
        dfs = []
        if suffix == '.csv':
            for f in tqdm(file_paths, desc='Files Read: '):
                dfs.append(pd.read_csv(f, dtype=dtypes))
        else:
            for f in tqdm(file_paths, desc='Files Read: '):
                dfs.append(pd.read_json(f, dtype=dtypes, lines=True))
        
        df_train = pd.concat(dfs)
        
        print(f'Dataframe finished for train table at {prefix} with'
            f' {len(df_train.columns):,d} columns and '
            f'{len(df_train):,d} rows.')
        
        return df_train

    def get_df_query_gcs(self, query, dtypes, file_format='csv', wait=True):
        prefix = self.export_query_gcs(query, file_format, wait)
        file_paths = self.get_table_gcs(prefix)
        df = self.get_df_files(file_paths, dtypes)
        
        return df