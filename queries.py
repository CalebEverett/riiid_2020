# adding  comment here to test commit from colab

import sys

class Queries:

    def __init__(self, DATASET, bq_client):
        self.DATASET = DATASET
        self.bq_client = bq_client
    
    def select_rows(self, table_id='train', limit=100):
        return f"""
            SELECT *
            FROM {self.DATASET}.{table_id}
            LIMIT {limit}
        """, sys._getframe().f_code.co_name + '_'


    def update_task_container_id(self, table_id='train', column_id_orig='task_container_id_orig'):
        return f"""
            UPDATE {self.DATASET}.{table_id}
            SET {column_id_orig} = task_container_id
            WHERE true;

            UPDATE {self.DATASET}.{table_id} t
            SET task_container_id = target.calc
            FROM (
              SELECT row_id, DENSE_RANK()
                OVER (
                  PARTITION BY user_id
                  ORDER BY timestamp
                ) - 1 calc
              FROM {self.DATASET}.{table_id}
            ) target
            WHERE target.row_id = t.row_id
        """, sys._getframe().f_code.co_name + '_'


    def create_train_sample(self, suffix='sample', user_id_max=50000):
        return f"""
            CREATE OR REPLACE TABLE {self.DATASET}.train_{suffix} AS
            SELECT *
            FROM {self.DATASET}.train
            WHERE user_id <= {user_id_max}
            ORDER BY user_id, task_container_id, row_id
        """, sys._getframe().f_code.co_name + '_'


    def select_train(self, columns=['*'], user_id_max=50000,
                     excl_lectures=False, table_id='train'):
        
        where_condition = f'user_id <= {user_id_max}' if user_id_max else 'true'
        where_condition = (where_condition + ' AND content_type_id = 0'
                           if excl_lectures else where_condition)
        
        return f"""
            SELECT {(', ').join(columns)}
            FROM {self.DATASET}.{table_id} t
            LEFT JOIN data.questions q
            ON t.content_id = q.question_id
            WHERE {where_condition}
            ORDER BY user_id, task_container_id, row_id
        """, sys._getframe().f_code.co_name + '_'
    

    def update_answered_incorrectly(self, table_id='train'):
        """Sets annswered_incorrectly to inverse of answered_correctly for questions.
        Sets answered_correctly to 0 for lectures so window totals for correct and
        incorrect are caculated correctly, including lectures.
        """
    
        return f"""
            UPDATE {self.DATASET}.{table_id}
            SET answered_incorrectly = 0
            WHERE true;

            UPDATE {self.DATASET}.{table_id}
            SET answered_incorrectly = 1 - answered_correctly
            WHERE content_type_id = 0;

            UPDATE {self.DATASET}.{table_id}
            SET answered_correctly = 0
            WHERE content_type_id = 1;
        """, sys._getframe().f_code.co_name + '_'


    def update_questions_tag__0(self):
        return f"""
            UPDATE data.questions
            SET tag__0 = tags[OFFSET(0)]
            WHERE true;
        """, sys._getframe().f_code.co_name + '_'    
    
    
    def update_train_window(self, table_id='train', source_column_id='answered_correctly',
                         update_column_id=None, window=0):
        """Calculates aggregate over preceding task_container_ids, limited
        to `window number` of task_container_ids unless window is 0 and then
        includes all task_container_ids.
        """
        
        return f"""            
            UPDATE {self.DATASET}.{table_id} t
            SET {update_column_id} = source.calc
            FROM (
              SELECT row_id, SUM({source_column_id})
                OVER (
                    PARTITION BY user_id
                    ORDER BY task_container_id
                    RANGE BETWEEN {window if window else 'UNBOUNDED'} PRECEDING AND 1 PRECEDING
                  ) calc
              FROM {self.DATASET}.{table_id}
              ORDER BY user_id, task_container_id, row_id
              ) source
             WHERE t.row_id = source.row_id;

            UPDATE {self.DATASET}.{table_id}
            SET {update_column_id} = 0
            WHERE {update_column_id} IS NULL;
        """, sys._getframe().f_code.co_name + '_'

    
    def update_question_correct_pct(self, column_id=None):
        return f"""  
            CREATE TEMP FUNCTION calcCorrectPct(c INT64, ic INT64) AS (
              CAST(c / (c + ic) * 100 AS INT64)
            );

            UPDATE data.questions q
            SET q.{column_id}_correct_pct = calcCorrectPct(c.c, c.ic)
            FROM (
                SELECT cq.{column_id}, SUM(answered_correctly) c, SUM(answered_incorrectly) ic
                FROM data.train t
                JOIN data.questions cq
                ON t.content_id = cq.question_id
                WHERE t.content_type_id = 0
                GROUP BY cq.{column_id}
            ) c
            WHERE q.{column_id} = c.{column_id}
        """, sys._getframe().f_code.co_name + '_'

    
    def select_user_id_rows(self, table_id='train', rows=30000):
        return f"""            
            SELECT user_id
            FROM {self.DATASET}.{table_id}
            WHERE row_id = {rows}
        """, sys._getframe().f_code.co_name + '_'
    

    def select_user_final_state(self):
        return f"""            
            SELECT t.user_id, t.task_container_id, t.answered_correctly_cumsum,
              t.answered_incorrectly_cumsum
            FROM (
              SELECT user_id,
                MAX(task_container_id) task_container_id,
                SUM(answered_correctly) answered_correctly_cumsum,
                SUM(answered_incorrectly) answered_incorrectly_cumsum,
              FROM data.train
              GROUP BY user_id
              ORDER BY user_id
              ) t
            JOIN (
              SELECT user_id,
                MAX(answered_correctly_roll) answered_correctly_roll,
                MAX(answered_incorrectly_roll) answered_incorrectly_roll
              FROM (
                SELECT user_id, task_container_id,
                  SUM(answered_correctly) OVER w answered_correctly_roll,
                  SUM(answered_incorrectly) OVER w answered_incorrectly_roll
                FROM data.train t2
                WINDOW w  AS (PARTITION BY user_id ORDER BY task_container_id RANGE BETWEEN 9 PRECEDING AND 0 PRECEDING)
                ) ji
                GROUP BY user_id
               ) j
            ON t.user_id = j.user_id
            ORDER BY t.user_id
        """, sys._getframe().f_code.co_name + '_'
    

    def select_user_roll_arrays(self, n_roll=11):
        """Work in progress. Still need to come up with a way to
        efficiently update rolling statistics during test prediction
        loop.
        """
        return f"""            
            SELECT user_id, ARRAY_AGG(task_container_id) task_container_id_rolloff,
                ARRAY_AGG(answered_correctly) answered_correctly_rolloff,
                ARRAY_AGG(answered_incorrectly) answered_incorrectly_rolloff,
            FROM (
              SELECT user_id, task_container_id,
                SUM(answered_correctly) answered_correctly,
                SUM(answered_incorrectly) answered_incorrectly,
                ROW_NUMBER() OVER(
                  PARTITION BY user_id
                  ORDER BY task_container_id DESC
                  ) row_number
              FROM data.train
              GROUP BY user_id, task_container_id
            ) t
            WHERE t.row_number < {n_roll}
            GROUP BY user_id
        """, sys._getframe().f_code.co_name + '_'        

    
    def done_cb(self, future):
        seconds = (future.ended - future.started).total_seconds()
        print(f'Job {future.job_id} finished in {seconds} seconds.')

    
    def run_query(self, query, job_id_prefix=None, wait=False):
        query_job = bq_client.query(query, job_id_prefix=job_id_prefix)
        print(f'Job {query_job.job_id} started.')
        query_job.add_done_callback(self.done_cb)
        if wait:
            query_job.result()
        
        return query_job