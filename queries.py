import sys

class Queries:
    def __init__(self, DATASET):
        self.DATASET = DATASET
    
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

    def create_table_folds(self, table_id='folds', n_folds=40,
                            pct_beg=0.1, pct_late=0.6, pct_late_start=0.5):
        return f"""
            DECLARE f INT64;

            CREATE OR REPLACE TABLE {self.DATASET}.{table_id} (
                user_id_s INT64,
                fold INT64,
                seq_start STRING,
                task_container_id_max INT64,
                task_container_id_min INT64
            );

            INSERT {self.DATASET}.{table_id} (user_id_s, task_container_id_max, fold)
            SELECT s.user_id, s.task_container_id_max, CAST(FLOOR(RAND() * {n_folds}) AS INT64) fold
            FROM (
            SELECT user_id,
                MAX(task_container_id) task_container_id_max
            FROM {self.DATASET}.train
            GROUP BY user_id
            ) s
            ORDER BY user_id;

            SET f = 0;
            WHILE f < {n_folds} DO

            UPDATE {self.DATASET}.{table_id} s
            SET seq_start = 'beg',
                task_container_id_min = 0
            WHERE fold = f AND RAND() < {pct_beg};

            UPDATE {self.DATASET}.{table_id} s
            SET seq_start = 'late',
                task_container_id_min = CAST(FLOOR(s.task_container_id_max * {pct_late_start} +
                    RAND() * (s.task_container_id_max * {1 - pct_late_start})) AS INT64)
            WHERE fold = f AND RAND() < {pct_late} AND task_container_id_min is NULL;

            UPDATE {self.DATASET}.{table_id} s
            SET seq_start = 'early',
                task_container_id_min = CAST(FLOOR(RAND() * s.task_container_id_max * {pct_late_start}) AS INT64) 
            WHERE fold = f AND task_container_id_min is NULL;

            SET f = f + 1;

            END WHILE;
        """, sys._getframe().f_code.co_name + '_'

    # def get_split_stats(self, table_id='splits'):
    #     return f"""
    #     SELECT s.fold, s.seq_start, MAX(s.task_container_id_min) task_container_id_min,
    #     MAX(t.task_container_id) task_container_id_max,
    #     SUM(t.answered_correctly) answered_correctly,
    #     SUM(t.answered_incorrectly) answered_inncorrectly,
    #     COUNT(t.row_id) row_id_count,
    #     ROUND(SUM(t.answered_correctly) * 100 / COUNT(t.row_id), 2) answered_correctly_pct
    #     FROM {self.DATASET}.{table_id} s
    #     JOIN {self.DATASET}.train t
    #     ON t.task_container_id >= s.task_container_id_min AND t.content_type_id = 0
    #     GROUP BY fold, seq_start
    #     ORDER BY fold, seq_start
    #     """, sys._getframe().f_code.co_name + '_'

    def update_folds(self, table_id='train', table_id_folds='folds'):
        return f"""
        UPDATE {self.DATASET}.{table_id} t
        SET t.fold = f.fold 
        FROM {self.DATASET}.{table_id_folds} f
        WHERE t.user_id = f.user_id_s
            AND t.task_container_id >= f.task_container_id_min
        """, sys._getframe().f_code.co_name + '_'
    
    def create_train_sample(self, table_id='train_sample', user_id_max=50000):
        return f"""
            CREATE TABLE {self.DATASET}.{table_id} AS
            SELECT *
            FROM {self.DATASET}.train
            WHERE user_id <= {user_id_max}
            ORDER BY user_id, task_container_id, row_id
        """, sys._getframe().f_code.co_name + '_'
    
    # ==== PRIMARY TRAIN DATAFRAME ====
    def select_train(self, columns=['*'], folds=[0],
                     excl_lectures=False, table_id='train',
                     table_id_folds='folds', limit=None): 

        folds = (' OR ').join([f'fold = {f}' for f  in folds])
        limit = f'LIMIT {limit}' if limit else ''
        excl_lectures = ' AND content_type_id = 0' if excl_lectures else ''

        return f"""
            SELECT {(', ').join(columns)}
            FROM {self.DATASET}.{table_id} t
            LEFT JOIN {self.DATASET}.content_tags ct
            ON t.ql_id = ct.ql_id
            WHERE ({folds}){excl_lectures}
            ORDER BY t.user_id, task_container_id, row_id
            {limit}
        """, sys._getframe().f_code.co_name + '_'
    
    def update_answered_correctly(self, table_id='train'):
        """Sets answered_correctly to 0 for lectures so window totals
        are caculated correctly, including lectures.
        """
    
        return f"""
            UPDATE {self.DATASET}.{table_id}
            SET answered_correctly = 0
            WHERE content_type_id = 1;
        """, sys._getframe().f_code.co_name + '_'

    def update_questions_tags_null(self):
        return f"""
            UPDATE {self.DATASET}.questions
            SET tags = '189'
            WHERE tags IS NULL;
        """, sys._getframe().f_code.co_name + '_'    
    
    def create_table_content_tags(self):
        return f"""
            DROP TABLE IF EXISTS data.content_tags;

            CREATE TABLE data.content_tags (
                ql_id INT64,
                question_id INT64,
                lecture_id INT64,
                bundle_id INT64,
                correct_answer INT64,
                part INT64,
                tags STRING,
                tags_array ARRAY<INT64>,
                tag_0 INT64,
                tags_code INT64,
                part_correct_pct INT64,
                tag_0_correct_pct INT64,
                question_id_correct_pct INT64,
                tags_correct_pct int64
            );

            INSERT INTO data.content_tags (ql_id, question_id, lecture_id, bundle_id, 
                correct_answer, part, tags, tags_array)
            SELECT ROW_NUMBER() OVER(ORDER BY question_id) - 1, question_id, NULL,
                bundle_id, correct_answer, part, tags,
                ARRAY(SELECT CAST(tag AS INT64) FROM UNNEST(SPLIT(tags, ' ')) tag)
            FROM {self.DATASET}.questions;

            INSERT INTO data.content_tags (ql_id, question_id, lecture_id, part,
                tags, tags_array)
            SELECT 13522 + ROW_NUMBER() OVER(ORDER BY lecture_id), NULL,
                lecture_id, part, CAST(tag AS STRING), [tag]
            FROM {self.DATASET}.lectures;

            UPDATE {self.DATASET}.content_tags
            SET tag_0 = tags_array[OFFSET(0)]
            WHERE true;

            UPDATE {self.DATASET}.content_tags
            SET tags_code = idx
            FROM (
                SELECT idx - 1 idx, tag
                FROM (
                    WITH unique_tags AS (SELECT DISTINCT tags tag
                    FROM data.content_tags
                    WHERE question_id IS NOT NULL
                    ORDER BY tags
                    )
                    SELECT tag, ROW_NUMBER() OVER(ORDER BY tag) idx
                    FROM unique_tags
                )
            )
            WHERE tag = tags;
        """, sys._getframe().f_code.co_name + '_'    

    def update_ql_id(self, table_id='train', table_id_ct='content_tags'):
        return f"""
            UPDATE {self.DATASET}.{table_id} t
            SET t.ql_id = ct.ql_id
            FROM {self.DATASET}.{table_id_ct} ct
            WHERE t.content_id = ct.question_id
                AND t.content_type_id = 0;

            UPDATE {self.DATASET}.{table_id} t
            SET t.ql_id = ct.ql_id
            FROM {self.DATASET}.{table_id_ct} ct
            WHERE t.content_id = ct.lecture_id
                AND t.content_type_id = 1;
        """, sys._getframe().f_code.co_name + '_'

    # def update_questions_tags_array(self):
    #     return f"""
    #         UPDATE {self.DATASET}.questions
    #         SET tags_array = ARRAY(
    #             SELECT CAST(tag AS INT64)
    #             FROM UNNEST(SPLIT(tags, ' ')) tag
    #         )
    #         WHERE true;
    #     """, sys._getframe().f_code.co_name + '_'
    
    # def update_questions_tag__0(self):
    #     return f"""
    #         UPDATE {self.DATASET}.content_tags
    #         SET tag__0 = tags_array[OFFSET(0)]
    #         WHERE true;
    #     """, sys._getframe().f_code.co_name + '_'

    # def update_questions_tags_code(self):
    #     return f"""
    #     UPDATE {self.DATASET}.questions
    #     SET tags_code = idx
    #     FROM (
    #         SELECT idx - 1 idx, tag
    #         FROM (
    #             WITH unique_tags AS (SELECT DISTINCT tags tag
    #             FROM {self.DATASET}.questions
    #             ORDER BY tags
    #             )
    #             SELECT tag, ROW_NUMBER() OVER(ORDER BY tag) idx
    #             FROM unique_tags
    #         )
    #     )
    #     WHERE tag = tags;
    #     """, sys._getframe().f_code.co_name + '_'
    
    def update_train_window_containers_pqet(self, table_id='train'):
        return f"""
        UPDATE {self.DATASET}.{table_id} t
        SET 
            pqet_sec = CAST(ROUND(prior_question_elapsed_time / 1000) AS INT64),
            pqet_sec_rollavg = calc.pqet_sec_rollavg
        FROM (
            SELECT
                row_id,
                IFNULL(CAST(ROUND((AVG(prior_question_elapsed_time) OVER(b) / 1000)) AS INT64), 0) pqet_sec_rollavg
            FROM {self.DATASET}.{table_id}
            WHERE content_type_id = 0
            WINDOW
                a AS (PARTITION BY user_id ORDER BY task_container_id),
                b AS (a RANGE BETWEEN 3 PRECEDING AND 0 PRECEDING)
        ) calc
        WHERE calc.row_id = t.row_id;

        UPDATE {self.DATASET}.{table_id}
        SET
            pqet_sec_rollavg = 0, 
            pqet_sec = 0, 
        WHERE content_type_id = 1;
        """, sys._getframe().f_code.co_name + '_'

    def update_train_window_containers_session(self, table_id='train', session_hours=72):
        return f"""
        UPDATE {self.DATASET}.{table_id} t
        SET 
            ts_minute = CAST(ROUND(timestamp / 60000) AS INT64),
            session_minute_max = calc.session_minute_max,
            session = calc.session,
            ac_cumsum_session = calc.ac_cumsum_session,
            r_cumcnt_session = calc.r_cumcnt_session,
            aic_cumsum_session = calc.r_cumcnt_session - calc.ac_cumsum_session,
            ac_cumsum_pct_session = IFNULL(CAST(SAFE_DIVIDE(calc.ac_cumsum_session * 100,
                 calc.r_cumcnt_session) AS INT64), -1),
            lectures_cumcnt_session = calc.lectures_cumcnt_session
        FROM (
            SELECT
                row_id, session_minute_max, session,
                IFNULL(SUM(answered_correctly) OVER (f), 0) ac_cumsum_session,
                IFNULL(SUM(CAST(content_type_id = 0 AS INT64)) OVER (f), 0) r_cumcnt_session,
                IFNULL(SUM(content_type_id) OVER (f), 0) lectures_cumcnt_session
            FROM (
                SELECT
                    user_id, task_container_id, row_id, answered_correctly, content_type_id,
                    CAST(MAX(ROUND(timestamp / 60000) - ts_minute_rollmax) OVER (d) AS INT64) session_minute_max,
                    IFNULL(SUM(session_flag) OVER (c), 0) session
                FROM (
                    SELECT
                        user_id, task_container_id, row_id, timestamp, answered_correctly, content_type_id,
                        IFNULL(CAST(ROUND((MAX(timestamp) OVER(b) / 60000)) AS INT64), 0) ts_minute_rollmax,
                        IF((timestamp - MAX(timestamp) OVER(b)) > (1000 * 60 * 60 * {session_hours}), 1, 0) session_flag
                    FROM {self.DATASET}.{table_id}
                    WINDOW
                        a AS (PARTITION BY user_id ORDER BY task_container_id),
                        b AS (a RANGE BETWEEN 1 PRECEDING AND 1 PRECEDING)
                )
                WINDOW
                    c AS (PARTITION BY user_id ORDER BY task_container_id),
                    d AS (c RANGE BETWEEN 10 PRECEDING AND 0 PRECEDING)
            )
            WINDOW
                e AS (PARTITION BY user_id, session ORDER BY task_container_id),
                f AS (e RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) 
        ) calc
        WHERE calc.row_id = t.row_id
        """, sys._getframe().f_code.co_name + '_'

    def update_train_window_containers(self, table_id='train'):
        return f"""
        UPDATE {self.DATASET}.{table_id} t
        SET
            -- user
            ac_cumsum = calc.ac_cumsum,
            r_cumcnt = calc.r_cumcnt,
            aic_cumsum = calc.r_cumcnt - calc.ac_cumsum,
            ac_cumsum_pct = IFNULL(CAST(SAFE_DIVIDE(calc.ac_cumsum * 100,
                 calc.r_cumcnt) AS INT64), -1),
            lectures_cumcnt = calc.lectures_cumcnt,
            
            -- content_id
            ac_cumsum_content_id = calc.ac_cumsum_content_id,
            r_cumcnt_content_id = calc.r_cumcnt_content_id,
            aic_cumsum_content_id = calc.r_cumcnt_content_id - calc.ac_cumsum_content_id,
            ac_cumsum_pct_content_id = IFNULL(CAST(SAFE_DIVIDE(calc.ac_cumsum_content_id * 100,
                calc.r_cumcnt_content_id)AS INT64), -1),
            
            -- part
            ac_cumsum_part = calc.ac_cumsum_part,
            r_cumcnt_part = calc.r_cumcnt_part,
            aic_cumsum_part = calc.r_cumcnt_part - calc.ac_cumsum_part,
            ac_cumsum_pct_part = IFNULL(CAST(SAFE_DIVIDE(calc.ac_cumsum_part * 100,
                 calc.r_cumcnt_part) AS INT64), -1),
            lectures_cumcnt_part = calc.lectures_cumcnt_part,
            
            -- clipped rows
            r_cumcnt_clip = IF(calc.r_cumcnt > 300, 300, calc.r_cumcnt)
        FROM (
            SELECT
                row_id,
                
                -- user
                IFNULL(SUM(answered_correctly) OVER (b), 0) ac_cumsum,
                IFNULL(SUM(CAST(content_type_id = 0 AS INT64)) OVER (b), 0) r_cumcnt,
                IFNULL(SUM(content_type_id) OVER (b), 0) lectures_cumcnt,
                
                -- content_id
                IFNULL(SUM(answered_correctly) OVER (e), 0) ac_cumsum_content_id,
                IFNULL(SUM(CAST(content_type_id = 0 AS INT64)) OVER (e), 0) r_cumcnt_content_id,
                
                -- part
                IFNULL(SUM(answered_correctly) OVER (g), 0) ac_cumsum_part,
                IFNULL(SUM(CAST(content_type_id = 0 AS INT64)) OVER (g), 0) r_cumcnt_part,
                IFNULL(SUM(content_type_id) OVER (g), 0) lectures_cumcnt_part,
            FROM {self.DATASET}.{table_id} t2
            LEFT JOIN {self.DATASET}.content_tags c
                ON t2.ql_id = c.ql_id
            WINDOW
                a AS (PARTITION BY user_id ORDER BY task_container_id),
                b AS (a RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
                d AS (PARTITION BY user_id, content_id ORDER BY task_container_id),
                e AS (d RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
                f AS (PARTITION BY user_id, part ORDER BY task_container_id),
                g AS (f RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
            ORDER BY user_id, task_container_id, row_id
        ) calc
        WHERE calc.row_id = t.row_id
        """, sys._getframe().f_code.co_name + '_'
   
    def update_train_window_containers_tag_0(self, table_id='train'):
        """This calcs the sum of prior answered correctly and incorrectly
            for all occurrences of the first tag, including instances where
            the tag is not first."""

        return f"""
        UPDATE {self.DATASET}.{table_id} t
        SET
            ac_cumsum_tag_0 = calc.ac_cumsum_tag_0,
            r_cumcnt_tag_0 = calc.r_cumcnt_tag_0,
            aic_cumsum_tag_0 = calc.r_cumcnt_tag_0 - calc.ac_cumsum_tag_0,
            ac_cumsum_pct_tag_0 = IFNULL(CAST(SAFE_DIVIDE(calc.ac_cumsum_tag_0 * 100,
                calc.r_cumcnt_tag_0) AS INT64), -1),
            lectures_cumcnt_tag_0 = calc.lectures_cumcnt_tag_0
        FROM (
        WITH tags_table AS (
            SELECT ql_id, tag_0, tags_array,
            FROM {self.DATASET}.content_tags
        )
        SELECT user_id, row_id, tag, tag_0,
            IFNULL(SUM(answered_correctly) OVER(b), 0) ac_cumsum_tag_0,
            IFNULL(SUM(CAST(content_type_id = 0 AS INT64)) OVER(b), 0) r_cumcnt_tag_0,
            IFNULL(SUM(content_type_id) OVER(b), 0) lectures_cumcnt_tag_0
        FROM tags_table
        JOIN UNNEST(tags_table.tags_array) AS tag
        JOIN {self.DATASET}.{table_id} t2
        ON tags_table.ql_id = t2.ql_id
        WINDOW
            a AS (PARTITION BY user_id, tag ORDER BY task_container_id),
            b AS (a RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
        ORDER BY user_id, task_container_id, row_id, tag
        ) calc
        WHERE t.row_id = calc.row_id
            AND calc.tag_0 = calc.tag
        """, sys._getframe().f_code.co_name + '_'    
    
    def update_train_window_containers_tags(self, table_id='train'):
        """This calcs the sum of prior answered correctly and incorrectly
            for all of the individual tags attached to each record."""

        return f"""
        UPDATE {self.DATASET}.{table_id} t
        SET
            ac_cumsum_tags = calc.ac_cumsum_tags,
            r_cumcnt_tags = calc.r_cumcnt_tags,
            aic_cumsum_tags = calc.r_cumcnt_tags - calc.ac_cumsum_tags,
            ac_cumsum_pct_tags = IFNULL(CAST(SAFE_DIVIDE(calc.ac_cumsum_tags * 100,
                calc.r_cumcnt_tags) AS INT64), -1),
            lectures_cumcnt_tags = calc.lectures_cumcnt_tags
        FROM (
        SELECT row_id,
            SUM(ac_cumsum_tags) ac_cumsum_tags,
            SUM(r_cumcnt_tags) r_cumcnt_tags,
            SUM(lectures_cumcnt_tags) lectures_cumcnt_tags
        FROM (
            WITH tags_table AS (
                SELECT ql_id, tags, tags_array,
                FROM {self.DATASET}.content_tags
            )
            SELECT user_id, task_container_id, row_id,
                IFNULL(SUM(answered_correctly) OVER(b), 0) ac_cumsum_tags,
                IFNULL(SUM(CAST(content_type_id = 0 AS INT64)) OVER(b), 0) r_cumcnt_tags,
                IFNULL(SUM(content_type_id) OVER(b), 0) lectures_cumcnt_tags
            FROM tags_table
            CROSS JOIN UNNEST(tags_table.tags_array) AS tag
            JOIN {self.DATASET}.{table_id} t
            ON tags_table.ql_id = t.ql_id
            WINDOW
                a AS (PARTITION BY user_id, tag ORDER BY task_container_id),
                b AS (a RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
            ORDER BY user_id, task_container_id, row_id, tag
        )
        GROUP BY row_id
        ORDER BY row_id
        ) calc
        WHERE t.row_id = calc.row_id  
        """, sys._getframe().f_code.co_name + '_'    
    
    def update_train_window_rows(self, table_id='train', window=10):
        """Calculates aggregate over window number of rows with task_container_id
        less than task_container_id of current row.
        """

        return f"""            
        UPDATE {self.DATASET}.{table_id} u
        SET answered_correctly_rollsum = IFNULL(calc.answered_correctly_rollsum, 0),
            roll_count = IFNULL(calc.row_id_roll_count, 0)
        FROM (
        SELECT t.row_id,
            COUNT(j2.row_id) row_id_rollcount,
            SUM(j2.answered_correctly) answered_correctly_rollsum
        FROM {self.DATASET}.{table_id} t
        JOIN (
            SELECT user_id, task_container_id, MIN(row_id) min_row
            FROM {self.DATASET}.{table_id}
            GROUP BY user_id, task_container_id
        ) j ON (
            j.user_id = t.user_id
            AND j.task_container_id = t.task_container_id
        )
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

    def update_answered_correctly_cumsum_upto(self, table_id='train', no_upto=10):        
        return f"""            
        UPDATE {self.DATASET}.{table_id} t
        SET ac_cumsum_upto = IF(r_cumcnt <= {no_upto}, ac_cumsum, m.ac_max),
            r_cumcnt_upto = IF(r_cumcnt <= {no_upto}, r_cumcnt, m.rc_max),
            aic_cumsum_upto = IF(r_cumcnt <= {no_upto}, r_cumcnt - ac_cumsum,
                m.rc_max - m.ac_max),
            ac_cumsum_pct_upto = IFNULL(CAST(
                SAFE_DIVIDE(
                    IF(r_cumcnt <= {no_upto}, ac_cumsum, m.ac_max) * 100,
                    IF(r_cumcnt <= {no_upto}, r_cumcnt, m.rc_max)
                ) AS INT64), -1)
        FROM (
            SELECT user_id, MAX(ac_cumsum) ac_max,
                MAX(r_cumcnt) rc_max,
            FROM {self.DATASET}.{table_id}
            WHERE r_cumcnt <= {no_upto}
            GROUP BY user_id
        ) m
        WHERE t.user_id = m.user_id
        """, sys._getframe().f_code.co_name + '_'

    def update_content_tags_correct_pct(self, column_id=None):        
        return f"""
        UPDATE {self.DATASET}.content_tags c
        SET {column_id}_correct_pct = calc.{column_id}_correct_pct
        FROM (
            SELECT
                {column_id},
                IFNULL(CAST(SUM(answered_correctly) * 100 / COUNT(answered_correctly) AS INT64), -1) {column_id}_correct_pct
            FROM {self.DATASET}.train t
            JOIN  {self.DATASET}.content_tags c2
            ON t.ql_id = c2.ql_id AND t.content_type_id = 0
            GROUP BY c2.{column_id}
        ) calc
        WHERE c.{column_id} = calc.{column_id}
        """, sys._getframe().f_code.co_name + '_'

    def select_user_final_state(self, table_id='train', no_upto=10):
        return f"""            

        SELECT t.user_id, ac_cumsum, ac_cumsum_upto,
            r_cumcnt - ac_cumsum aic_cumsum,
            r_cumcnt_upto - ac_cumsum_upto aic_cumsum_upto,
            lectures_cumcnt, r_cumcnt, r_cumcnt_upto
        FROM (
            SELECT user_id, SUM(answered_correctly) ac_cumsum,
                    SUM(CAST(content_type_id = 0 AS INT64)) r_cumcnt,
                    SUM(content_type_id) lectures_cumcnt
            FROM {self.DATASET}.{table_id}
            GROUP BY user_id
            ORDER BY user_id
        ) t
        JOIN (
            SELECT user_id, FIRST_VALUE(ac_cumsum_upto) OVER(w) ac_cumsum_upto,
                 FIRST_VALUE(r_cumcnt_upto) OVER(w) r_cumcnt_upto,
                 ROW_NUMBER() OVER (w) row_number
            FROM {self.DATASET}.train
            WHERE r_cumcnt_upto <= {no_upto}
            WINDOW
                w AS (PARTITION BY user_id ORDER BY row_id DESC)
            ORDER BY user_id
        ) u ON t.user_id = u.user_id AND u.row_number = 1
        ORDER BY user_id
        """, sys._getframe().f_code.co_name + '_'

    def select_user_content_final_state(self, table_id='train'):
        return f"""
        SELECT user_id, content_id,
            SUM(answered_correctly) ac_cumsum_content_id,
            SUM(CAST(content_type_id = 0 AS INT64)) r_cumcnt_content_id,
            SUM(CAST(content_type_id = 0 AS INT64)) - SUM(answered_correctly) aic_cumsum_content_id
        FROM {self.DATASET}.{table_id}
        GROUP BY user_id, content_id
        ORDER BY user_id, content_id
        """, sys._getframe().f_code.co_name + '_'
    
    def select_users_tag_final_state(self, table_id='train'):
        return f"""
        WITH tags_table AS (
        SELECT ql_id, tags, tags_array,
        FROM {self.DATASET}.content_tags
        )
        SELECT user_id, tag, SUM(answered_correctly) ac_cumsum_tag,
            SUM(CAST(content_type_id = 0 AS INT64)) r_cumcnt_tag,
            SUM(CAST(content_type_id = 0 AS INT64)) - SUM(answered_correctly) aic_cumsum_tag,
            SUM(content_type_id) lectures_cumcnt_tag
        FROM tags_table
        JOIN UNNEST(tags_table.tags_array) AS tag
        JOIN {self.DATASET}.{table_id} t
        ON t.ql_id = tags_table.ql_id
        GROUP BY user_id, tag
        ORDER BY user_id, tag
        """, sys._getframe().f_code.co_name + '_'