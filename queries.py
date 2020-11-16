import sys

class Queries:
    def __init__(self, DATASET):
        self.DATASET = DATASET
    
    def select_rows(self, table_id='train', limit=100):
        return f"""
            SELECT *
            FROM {self.DATASET}.{table_id}
            LIMIT {limit}
        """, sys._getframe().f_code.co_name + '_'

    def update_missing_values(self, table_id='train', column_id=None, value=None):
        return f"""
            UPDATE {self.DATASET}.{table_id}
            SET {column_id} = {value}
            WHERE {column_id} is NULL;
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
            LEFT JOIN {self.DATASET}.questions q
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
    
    def update_train_window_containers(self, table_id='train'):
        return f"""            
        UPDATE {self.DATASET}.{table_id} t
        SET answered_correctly_cumsum = IFNULL(calc.answered_correctly_cumsum, 0),
            answered_incorrectly_cumsum = IFNULL(calc.answered_incorrectly_cumsum, 0),
            lectures_cumcount = IFNULL(calc.lectures_cumcount, 0),
            prior_question_elapsed_time_rollavg = IFNULL(calc.prior_question_elapsed_time_rollavg, 0),
            answered_correctly_content_id_cumsum = IFNULL(calc.answered_correctly_content_id_cumsum, 0),
            answered_incorrectly_content_id_cumsum = IFNULL(calc.answered_incorrectly_content_id_cumsum, 0)
        FROM (
        SELECT row_id,
            SUM(answered_correctly) OVER (b) answered_correctly_cumsum,
            SUM(answered_incorrectly) OVER (b) answered_incorrectly_cumsum,
            SUM(content_type_id) OVER (b) lectures_cumcount,
            AVG(prior_question_elapsed_time) OVER (c) prior_question_elapsed_time_rollavg,
            SUM(answered_correctly) OVER (e) answered_correctly_content_id_cumsum,
            SUM(answered_incorrectly) OVER (e) answered_incorrectly_content_id_cumsum
        FROM {self.DATASET}.{table_id}
        WINDOW
            a AS (PARTITION BY user_id ORDER BY task_container_id),
            b AS (a RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
            c AS (a RANGE BETWEEN 3 PRECEDING AND 0 PRECEDING),
            d AS (PARTITION BY user_id, content_id ORDER BY task_container_id),
            e AS (d RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
        ORDER BY user_id, task_container_id, row_id
        ) calc
        WHERE calc.row_id = t.row_id
        """, sys._getframe().f_code.co_name + '_'

    def update_train_window_rows(self, table_id='train', window=10):
        """Calculates aggregate over window number of rows with task_container_id
        less than task_container_id of current row.
        """

        return f"""            
        UPDATE {self.DATASET}.{table_id} u
        SET answered_correctly_rollsum = IFNULL(calc.answered_correctly_rollsum, 0),
            answered_incorrectly_rollsum = IFNULL(calc.answered_incorrectly_rollsum, 0)
        FROM (
        SELECT t.row_id,
            COUNT(j2.row_id) row_id_rollcount,
            SUM(j2.answered_correctly) answered_correctly_rollsum,
            SUM(j2.answered_incorrectly) answered_incorrectly_rollsum,
        FROM {self.DATASET}.{table_id} t
        JOIN (
            SELECT user_id, task_container_id, MIN(row_id) min_row
            FROM {self.DATASET}.{table_id}
            GROUP BY user_id, task_container_id
        ) j ON (j.user_id = t.user_id AND j.task_container_id = t.task_container_id)
        LEFT JOIN {self.DATASET}.{table_id} j2 ON (
            j2.user_id = t.user_id
            AND j2.task_container_id < t.task_container_id
            AND j2.row_id >= (j.min_row - {window + 1})
        )
        GROUP BY t.user_id, t.task_container_id, t.row_id
        ) calc
        WHERE
        calc.row_id = u.row_id
        """, sys._getframe().f_code.co_name + '_'


    def update_answered_correctly_cumsum_upto(self, table_id='train'):        
        return f"""            
        UPDATE {self.DATASET}.{table_id} t
        SET answered_correctly_cumsum_upto = IF(row_number < 11, r.answered_correctly_cumsum, m.ac_max)
        FROM (
        SELECT user_id, row_id, answered_correctly_cumsum,
            ROW_NUMBER() OVER(W) row_number,
        FROM {self.DATASET}.{table_id}
        WHERE content_type_id = 0
        WINDOW
            w AS (PARTITION BY user_id ORDER BY row_id)
        ) r
        JOIN (
        SELECT user_id, MAX(answered_correctly_cumsum) ac_max
        FROM (
            SELECT user_id, row_id, answered_correctly_cumsum,
            ROW_NUMBER() OVER(W) row_number,
            FROM {self.DATASET}.{table_id}
            WINDOW
                w AS (PARTITION BY user_id ORDER BY row_id)
        )
        WHERE row_number < 11
        GROUP BY user_id
        ) m
        ON (m.user_id = r.user_id)
        WHERE r.row_id = t.row_id
        """, sys._getframe().f_code.co_name + '_'

    def update_correct_cumsum_pct(self, column_id_correct=None,
                                  column_id_incorrect=None,
                                  update_column_id=None, table_id='train'):
        return f"""
            CREATE TEMP FUNCTION calcCorrectPct(c INT64, ic INT64) AS (
              CAST(SAFE_DIVIDE(c, (c + ic)) * 100 AS INT64)
            );

            UPDATE {self.DATASET}.{table_id}
            SET {update_column_id} =
                calcCorrectPct({column_id_correct}, {column_id_incorrect})
            WHERE true;
            
            UPDATE {self.DATASET}.{table_id}
            SET {update_column_id} = 0
            WHERE {update_column_id} IS NULL;
        """, sys._getframe().f_code.co_name + '_'

    def update_question_correct_pct(self, column_id):
        return f"""  
            CREATE TEMP FUNCTION calcCorrectPct(c INT64, ic INT64) AS (
              CAST(SAFE_DIVIDE(c, (c + ic)) * 100 AS INT64)
            );

            UPDATE {self.DATASET}.questions q
            SET q.{column_id}_correct_pct = calcCorrectPct(c.c, c.ic)
            FROM (
                SELECT cq.{column_id}, SUM(answered_correctly) c, SUM(answered_incorrectly) ic
                FROM {self.DATASET}.train t
                JOIN {self.DATASET}.questions cq
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
    
    def select_user_final_state(self, table_id='train'):
        return f"""            
        CREATE TEMP FUNCTION calcCorrectPct(c INT64, ic INT64) AS (
        IFNULL(CAST(SAFE_DIVIDE(c, (c + ic)) * 100 AS INT64), 0)
        );
        
        SELECT *, calcCorrectPct(answered_correctly_cumsum, answered_incorrectly_cumsum) answered_correctly_cumsum_pct,
        calcCorrectPct(answered_correctly_rollsum, answered_incorrectly_rollsum) answered_correctly_rollsum_pct
        FROM (
        SELECT row_id, user_id, answered_correctly_cumsum_upto, content_type_id,
            SUM(answered_correctly) OVER (b) answered_correctly_cumsum,
            SUM(answered_incorrectly) OVER (b) answered_incorrectly_cumsum,
            SUM(answered_correctly) OVER (d) answered_correctly_rollsum,
            SUM(answered_incorrectly) OVER (d) answered_incorrectly_rollsum,
            SUM(content_type_id) OVER (b) lectures_cumcount,
            AVG(prior_question_elapsed_time) OVER (c) prior_question_elapsed_time_rollavg,
            ROW_NUMBER() OVER(y) row_no_desc,
            SUM(answered_correctly + answered_incorrectly) OVER (d) answer_row_id_rollcount,
            SUM(answered_correctly + answered_incorrectly) OVER (c) time_row_id_rollcount,
            SUM(answered_correctly + answered_incorrectly) OVER (a) question_row_id_rollcount,
        FROM {self.DATASET}.{table_id}
        WINDOW
            x AS (PARTITION BY user_id),
            y AS (x ORDER BY task_container_id DESC, row_id DESC),
            a AS (x ORDER BY task_container_id),
            b AS (a ROWS BETWEEN UNBOUNDED PRECEDING AND 0 PRECEDING),
            c AS (a RANGE BETWEEN 3 PRECEDING AND 0 PRECEDING),
            d AS (a ROWS BETWEEN 9 PRECEDING AND 0 PRECEDING)
        )
        WHERE row_no_desc = 1 AND content_type_id = 0
        ORDER BY user_id
        """, sys._getframe().f_code.co_name + '_'

    def select_user_content_final_state(self, table_id='train'):
        return f"""            
        SELECT user_id, content_id, SUM(answered_correctly) answered_correctly,
            SUM(answered_incorrectly) answered_incorrectly,
        FROM {self.DATASET}.{table_id}
        WHERE content_type_id = 0
        GROUP BY user_id, content_id
        ORDER BY user_id, content_id
        """, sys._getframe().f_code.co_name + '_'
    