import sys

class Queries:
    def __init__(self, DATASET):
        self.DATASET = DATASET
        
    # =======================================
    # ===== QUESTIONS BEFORE TRANSFORMS =====
    # =======================================
    
    def update_missing_values(self, table_id='train', column_id=None, value=None):
        return f"""
            UPDATE {self.DATASET}.{table_id}
            SET {column_id} = {value}
            WHERE {column_id} is NULL;
        """, sys._getframe().f_code.co_name + '_'

    def create_table_content_tags(self):
        return f"""
            CREATE OR REPLACE TABLE data.content_tags (
                ql_id INT64,
                question_id INT64,
                lecture_id INT64,
                bundle_id INT64,
                correct_answer INT64,
                part INT64,
                tags STRING,
                tags_array ARRAY<INT64>,
                tags_code INT64,
                part_correct_pct INT64,
                question_id_correct_pct INT64,
                tags_correct_pct int64,
                part_tags_correct_pct INT64,
                part_pqet_avg INT64,
                question_id_pqet_avg INT64,
                tags_pqet_avg INT64,
                part_tags_pqet_avg INT64
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
    
    
    # ============================
    # ===== TRAIN TRANSFORMS =====
    # ============================ 
    
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

    def update_pqet_current(self, table_id='train'):
        return f"""
            UPDATE {self.DATASET}.{table_id} t
            SET t.pqet_current = CAST(p.pqet_current AS INT64)
            FROM (
                SELECT
                row_id, LAST_VALUE(prior_question_elapsed_time) OVER (
                  PARTITION BY user_id ORDER BY task_container_id_q
                  RANGE BETWEEN 1 FOLLOWING AND 1 FOLLOWING) pqet_current
                FROM {self.DATASET}.train            
                WHERE content_type_id = 0
            ) p
            WHERE t.row_id = p.row_id
            
        """, sys._getframe().f_code.co_name + '_'
    
    def update_ts_delta(self, table_id='train'):
        return f"""
            UPDATE {self.DATASET}.{table_id} t
            SET t.ts_delta = IFNULL(timestamp - p.ts_prior, -1)
            FROM (
                SELECT
                row_id, LAST_VALUE(timestamp) OVER (
                  PARTITION BY user_id ORDER BY task_container_id_q
                  RANGE BETWEEN 1 PRECEDING AND 1 PRECEDING) ts_prior
                FROM {self.DATASET}.train            
                WHERE content_type_id = 0
            ) p
            WHERE t.row_id = p.row_id
        """, sys._getframe().f_code.co_name + '_'

    def update_task_container_id(self, table_id='train',
                                   column_id='task_container_id',
                                   excl_lectures=False):
        excl_lec = 'WHERE content_type_id = 0' if excl_lectures else ''
        
        return f"""
            UPDATE {self.DATASET}.{table_id} t
            SET {column_id} = target.calc
            FROM (
              SELECT row_id, DENSE_RANK()
                OVER (
                  PARTITION BY user_id
                  ORDER BY timestamp
                ) - 1 calc
              FROM {self.DATASET}.{table_id}
              {excl_lec}
            ) target
            WHERE target.row_id = t.row_id
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
    
    def update_train_window_containers_session(self, table_id='train', session_hours=18):
        return f"""
        UPDATE {self.DATASET}.{table_id} t
        SET 
            session = calc.session,
            ac_cumsum_session = calc.ac_cumsum_session,
            r_cumcnt_session = calc.r_cumcnt_session,
            ac_cumsum_pct_session = IFNULL(CAST(SAFE_DIVIDE(calc.ac_cumsum_session * 100,
                 calc.r_cumcnt_session) AS INT64), -1),
            l_cumcnt_session = calc.l_cumcnt_session,
            pqet_cumavg_session = CAST(calc.pqet_cumavg_session AS INT64)
        FROM (
            SELECT
                row_id, session,
                IFNULL(SUM(answered_correctly) OVER (f), 0) ac_cumsum_session,
                IFNULL(SUM(CAST(content_type_id = 0 AS INT64)) OVER (f), 0) r_cumcnt_session,
                IFNULL(SUM(content_type_id) OVER (i), 0) l_cumcnt_session,
                IFNULL(AVG(pqet_current) OVER (f), 0) pqet_cumavg_session
            FROM (
                SELECT
                    user_id, task_container_id, task_container_id_q, row_id,
                    answered_correctly, content_type_id, pqet_current,
                    IFNULL(SUM(session_flag) OVER (c), 0) session
                FROM (
                    SELECT
                        user_id, task_container_id, task_container_id_q, row_id,
                        timestamp, answered_correctly, content_type_id, pqet_current,
                        IF(ts_delta > (1000 * 60 * 60 * {session_hours}), 1, 0) session_flag
                    FROM {self.DATASET}.{table_id}
                )
                WINDOW
                    c AS (PARTITION BY user_id ORDER BY task_container_id_q)
            )
            WINDOW
                e AS (PARTITION BY user_id, session ORDER BY task_container_id_q),
                f AS (e RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
                h AS (PARTITION BY user_id, session ORDER BY task_container_id),
                i AS (h RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
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
            ac_cumsum_pct = IFNULL(CAST(SAFE_DIVIDE(calc.ac_cumsum * 100,
                 calc.r_cumcnt) AS INT64), -1),
            l_cumcnt = calc.l_cumcnt,
            pqet_cumavg = CAST(calc.pqet_cumavg AS INT64),
            
            -- content_id
            ac_cumsum_content_id = calc.ac_cumsum_content_id,
            r_cumcnt_content_id = calc.r_cumcnt_content_id,
            ac_cumsum_pct_content_id = IFNULL(CAST(SAFE_DIVIDE(calc.ac_cumsum_content_id * 100,
                calc.r_cumcnt_content_id)AS INT64), -1),
            pqet_cumavg_content_id = CAST(calc.pqet_cumavg_content_id AS INT64),
            
            -- part
            ac_cumsum_part = calc.ac_cumsum_part,
            r_cumcnt_part = calc.r_cumcnt_part,
            ac_cumsum_pct_part = IFNULL(CAST(SAFE_DIVIDE(calc.ac_cumsum_part * 100,
                 calc.r_cumcnt_part) AS INT64), -1),
            l_cumcnt_part = calc.l_cumcnt_part,
            pqet_cumavg_part = CAST(calc.pqet_cumavg_part AS INT64),
            
            -- clipped rows
            r_cumcnt_clip = IF(calc.r_cumcnt > 300, 300, calc.r_cumcnt)
        FROM (
            SELECT
                row_id,
                
                -- user
                IFNULL(SUM(answered_correctly) OVER (b), 0) ac_cumsum,
                IFNULL(SUM(CAST(content_type_id = 0 AS INT64)) OVER (b), 0) r_cumcnt,
                IFNULL(SUM(content_type_id) OVER (b), 0) l_cumcnt,
                IFNULL(AVG(pqet_current) OVER (b), 0) pqet_cumavg,
                
                -- content_id
                IFNULL(SUM(answered_correctly) OVER (e), 0) ac_cumsum_content_id,
                IFNULL(SUM(CAST(content_type_id = 0 AS INT64)) OVER (e), 0) r_cumcnt_content_id,
                IFNULL(AVG(pqet_current) OVER (e), 0) pqet_cumavg_content_id,
                
                -- part
                IFNULL(SUM(answered_correctly) OVER (g), 0) ac_cumsum_part,
                IFNULL(SUM(CAST(content_type_id = 0 AS INT64)) OVER (g), 0) r_cumcnt_part,
                IFNULL(SUM(content_type_id) OVER (g), 0) l_cumcnt_part,
                IFNULL(AVG(pqet_current) OVER (g), 0) pqet_cumavg_part
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

    def update_train_window_containers_tags(self, table_id='train'):
        return f"""
        UPDATE data.train u
        SET
          --tags
          ac_cumsum_tags = calc.ac_cumsum_tags,
          r_cumcnt_tags = calc.r_cumcnt_tags,
          ac_cumsum_pct_tags = IFNULL(CAST(SAFE_DIVIDE(calc.ac_cumsum_tags * 100,
              calc.r_cumcnt_tags) AS INT64), -1),
          l_cumcnt_tags = calc.l_cumcnt_tags,
          pqet_cumavg_tags = IFNULL(CAST(SAFE_DIVIDE(calc.pqet_current_cumsum_tags,
            calc.r_cumcnt_tags) AS INT64), -1),

          --part_tags
          ac_cumsum_part_tags = calc.ac_cumsum_part_tags,
          r_cumcnt_part_tags = calc.r_cumcnt_part_tags,
          ac_cumsum_pct_part_tags = IFNULL(CAST(SAFE_DIVIDE(calc.ac_cumsum_part_tags * 100,
              calc.r_cumcnt_part_tags) AS INT64), -1),
          l_cumcnt_part_tags = calc.l_cumcnt_part_tags,
          pqet_cumavg_part_tags = IFNULL(CAST(SAFE_DIVIDE(calc.pqet_current_cumsum_part_tags,
            calc.r_cumcnt_part_tags) AS INT64), -1)
        
        FROM ( 
          SELECT
            row_id,
            --tags
            IFNULL(SUM(ac_cumcum_tags), 0) ac_cumsum_tags,
            IFNULL(SUM(r_cumcnt_tags), 0) r_cumcnt_tags,
            IFNULL(SUM(l_cumcnt_tags), 0) l_cumcnt_tags,
            IFNULL(SUM(pqet_current_cumsum_tags), 0) pqet_current_cumsum_tags,

            --part-tags
            IFNULL(SUM(ac_cumcum_part_tags), 0) ac_cumsum_part_tags,
            IFNULL(SUM(r_cumcnt_part_tags), 0) r_cumcnt_part_tags,
            IFNULL(SUM(l_cumcnt_part_tags), 0) l_cumcnt_part_tags,
            IFNULL(SUM(pqet_current_cumsum_part_tags), 0) pqet_current_cumsum_part_tags,

          FROM (
          SELECT
            row_id,
            --tags
            SUM(answered_correctly) OVER(b) ac_cumcum_tags,
            SUM(CAST(content_type_id = 0 AS INT64)) OVER(b) r_cumcnt_tags,
            SUM(content_type_id) OVER(b) l_cumcnt_tags,
            SUM(pqet_current) OVER(b) pqet_current_cumsum_tags,

            --part-tags
            SUM(answered_correctly) OVER(d) ac_cumcum_part_tags,
            SUM(CAST(content_type_id = 0 AS INT64)) OVER(d) r_cumcnt_part_tags,
            SUM(content_type_id) OVER(d) l_cumcnt_part_tags,
            SUM(pqet_current) OVER(d) pqet_current_cumsum_part_tags

          FROM data.tag_response
          WINDOW
              a AS (PARTITION BY user_id, tag ORDER BY task_container_id),
              b AS (a RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
              c AS (PARTITION BY user_id, part, tag ORDER BY task_container_id),
              d AS (c RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
          ORDER BY user_id, task_container_id, row_id, tag
          )
          GROUP BY row_id
        ) calc
        WHERE u.row_id = calc.row_id
        """, sys._getframe().f_code.co_name + '_'    
    
    def update_answered_correctly_cumsum_upto(self, table_id='train', no_upto=10):        
        return f"""            
        UPDATE {self.DATASET}.{table_id} t
        SET ac_cumsum_upto = IF(r_cumcnt <= {no_upto}, ac_cumsum, m.ac_max),
            r_cumcnt_upto = IF(r_cumcnt <= {no_upto}, r_cumcnt, m.rc_max),
            ac_cumsum_pct_upto = IFNULL(CAST(
                SAFE_DIVIDE(
                    IF(r_cumcnt <= {no_upto}, ac_cumsum, m.ac_max) * 100,
                    IF(r_cumcnt <= {no_upto}, r_cumcnt, m.rc_max)
                ) AS INT64), -1),
           pqet_cumavg_upto = CAST(IF(r_cumcnt <= {no_upto},
               pqet_cumavg, m.pqet_cumavg_upto) AS INT64)
        FROM (
            SELECT user_id, MAX(ac_cumsum) ac_max,
                MAX(r_cumcnt) rc_max,
                AVG(pqet_current) pqet_cumavg_upto
            FROM {self.DATASET}.{table_id}
            WHERE r_cumcnt <= {no_upto}
            GROUP BY user_id
        ) m
        WHERE t.user_id = m.user_id
        """, sys._getframe().f_code.co_name + '_'
    
    def create_tag_response(self):
        return f"""
        CREATE OR REPLACE TABLE {self.DATASET}.tag_response AS (
          WITH tags_table AS (
              SELECT ql_id, question_id, part, tags_array,
              FROM {self.DATASET}.content_tags
          )
          SELECT user_id, t.row_id, task_container_id, task_container_id_q,
              t.ql_id, content_type_id, part, tag, answered_correctly, pqet_current
          FROM tags_table
          CROSS JOIN UNNEST(tags_table.tags_array) AS tag
          JOIN {self.DATASET}.train t
          ON
            t.content_type_id = 0
            AND tags_table.ql_id = t.ql_id
          ORDER BY user_id, task_container_id, row_id, part, tag
        )
        """, sys._getframe().f_code.co_name + '_'
    
    def update_top_cids(self, top_content_ids):
        def get_top_c_q(it):
            i, t = it
            return (f'ac_cumsum_pct_top_cid_{i} = CAST(IFNULL(SAFE_DIVIDE(ac_cumsum_top_cid_{i} * 100, r_cumcnt_top_cid_{i}), -1) AS INT64)',
                    f'SUM(CAST(answered_correctly = 1 AND content_id = {t} AS INT64)) OVER (w) ac_cumsum_top_cid_{i},'
                    f'SUM(CAST(content_id = {t} AS INT64)) OVER (w) r_cumcnt_top_cid_{i}')

        sets, calcs = list(map(list, zip(*map(get_top_c_q, enumerate(top_content_ids)))))

        return f"""
        UPDATE {self.DATASET}.train t
        SET
          {(',').join(sets)}
        FROM (
          SELECT
            row_id,
            {(',').join(calcs)}
          FROM {self.DATASET}.train
          WINDOW
            w AS (PARTITION BY user_id ORDER BY task_container_id
            RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
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
    

    # =====================================
    # ===== QUESTION AFTER TRANSFORMS =====
    # =====================================
    
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

    def update_content_tags_pqet_avg(self, column_id=None):        
        return f"""
        UPDATE {self.DATASET}.content_tags c
        SET c.{column_id}_pqet_avg = IFNULL(CAST(pqet_avg AS INT64), -1)
        FROM (
            SELECT {column_id}, AVG(pqet_next) pqet_avg
            FROM {self.DATASET}.train t
            JOIN (
                SELECT
                    row_id, {column_id},
                    LAST_VALUE(prior_question_elapsed_time) OVER (b) pqet_next
                FROM {self.DATASET}.train t
                JOIN {self.DATASET}.content_tags c
                ON t.ql_id = c.ql_id
                WHERE content_type_id = 0
                WINDOW
                    a AS (PARTITION BY user_id ORDER BY task_container_id_q),
                    b AS (a RANGE BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
            ) calc
            ON t.row_id = calc.row_id AND t.content_type_id = 0
            GROUP BY {column_id}
        ) n
        WHERE c.{column_id} = n.{column_id}
        """, sys._getframe().f_code.co_name + '_'

    def update_content_tags_tags(self):
        """This one does both pct correct and pqet from intermediate table"""
        return f"""
        UPDATE data.content_tags u
        SET 
          tags_correct_pct = CAST(ac_cumsum * 100 / r_cumcnt AS INT64),
          tags_pqet_avg = CAST(pqet_cumsum / r_cumcnt AS INT64)
        FROM (
          SELECT
            ql_id,
            SUM(ac_cumsum) ac_cumsum,
            SUM(r_cumcnt) r_cumcnt,
            SUM(pqet_cumsum) pqet_cumsum
          FROM (
            SELECT tag,
              SUM(answered_correctly) ac_cumsum,
              COUNT(answered_correctly) r_cumcnt,
              SUM(pqet_current) pqet_cumsum
            FROM data.tag_response
            WHERE content_type_id = 0
            GROUP BY tag
          ) tr
          JOIN (
            SELECT ql_id, part, tags_array
            FROM data.content_tags
          ) ct ON tr.tag IN UNNEST(ct.tags_array)
          GROUP BY ql_id
        ) calc
        WHERE u.ql_id = calc.ql_id
        """, sys._getframe().f_code.co_name + '_'
    
    def update_content_tags_part_tags(self):
        """This one does both pct correct and pqet from intermediate table"""
        return f"""
        UPDATE data.content_tags u
        SET 
          part_tags_correct_pct = IFNULL(CAST(SAFE_DIVIDE(ac_cumsum * 100, r_cumcnt) AS INT64), -1),
          part_tags_pqet_avg = IFNULL(CAST(SAFE_DIVIDE(pqet_cumsum, r_cumcnt) AS INT64), -1)
        FROM (
          SELECT
            ql_id,
            SUM(ac_cumsum) ac_cumsum,
            SUM(r_cumcnt) r_cumcnt,
            SUM(pqet_cumsum) pqet_cumsum
          FROM (
            SELECT part, tag,
              SUM(answered_correctly) ac_cumsum,
              COUNT(answered_correctly) r_cumcnt,
              SUM(pqet_current) pqet_cumsum
            FROM data.tag_response
            WHERE content_type_id = 0
            GROUP BY part, tag
          ) tr
          JOIN (
            SELECT ql_id, part, tags_array
            FROM data.content_tags
          ) ct ON tr.part = ct.part AND tr.tag IN UNNEST(ct.tags_array)
          GROUP BY ql_id
          ORDER BY ql_id
        ) calc
        WHERE u.ql_id = calc.ql_id
        """, sys._getframe().f_code.co_name + '_'
    
    
    # ======================
    # ===== ROLL STATS =====
    # ======================    
    
    def create_roll_stats(self, win_lens, calc_list, n_prec=0, table_id='roll_stats'):
        wins = list(zip(win_lens, 'abcdefghij'))

        col_list = [c.split()[-1] for c in calc_list]
        col_list = [c.format(win_len=win_len) for c in col_list for win_len, _ in wins]
        create_list = [f'{c} INT64' for c in col_list]

        calc_list = [c.format(win_len=win_len, win_label=win_label)
                     for c in calc_list for win_len, win_label in wins]

        window_list = [f'{win_label} AS (w RANGE BETWEEN {win_len} PRECEDING AND {n_prec} PRECEDING)' for win_len, win_label in wins]

        return f"""
        DROP TABLE IF EXISTS {self.DATASET}.{table_id};

        CREATE TABLE {self.DATASET}.{table_id} (
            row_id_r INT64,
            {(',').join(create_list)}
        );

        INSERT INTO {self.DATASET}.{table_id}
            (row_id_r, {(',').join(col_list)})
        SELECT
            row_id,
            {(',').join(calc_list)}
        FROM {self.DATASET}.train
        WHERE task_container_id_q IS NOT NULL
        WINDOW
            w AS (PARTITION BY user_id ORDER BY task_container_id_q),
            {(',').join(window_list)}
        """, sys._getframe().f_code.co_name + '_'  
    
    def update_roll_stats(self, win_lens, calc_list, n_prec=0,
                          table_id='roll_stats', tid_col='task_container_id'):
        wins = list(zip(win_lens, 'abcdefghij'))

        col_list = [c.split()[-1] for c in calc_list]
        col_list = [c.format(win_len=win_len) for c in col_list for win_len, _ in wins]
        create_list = [f'ADD COLUMN {c} INT64' for c in col_list]
        set_list = [f'{c} = calc.{c}' for c in col_list]

        calc_list = [c.format(win_len=win_len, win_label=win_label)
                     for c in calc_list for win_len, win_label in wins]

        window_list = [f'{win_label} AS (w RANGE BETWEEN {win_len} PRECEDING AND {n_prec} PRECEDING)' for win_len, win_label in wins]

        return f"""
        ALTER TABLE {self.DATASET}.{table_id}
            {(',').join(create_list)};
            
        UPDATE {self.DATASET}.{table_id} r
        SET
            {(',').join(set_list)}
        FROM (
            SELECT
                row_id,
                {(',').join(calc_list)}
            FROM {self.DATASET}.train
            WINDOW
                w AS (PARTITION BY user_id ORDER BY {tid_col}),
                {(',').join(window_list)}
        ) calc
        WHERE row_id = row_id_r;
        """, sys._getframe().f_code.co_name + '_'
           

    # ===========================
    # ===== TOP CONTENT_IDS =====
    # ===========================
    
    def create_top_content_ids(self, top_content_ids, table_id='top_content_ids'):
        calc_list = [
            'IFNULL(SUM(CAST(answered_correctly = 1 AND content_id = {cid} AS INT64)) OVER (w), 0) ac_cumsum_top_cid_{cid}',
            'IFNULL(SUM(CAST(content_id = {cid} AS INT64)) OVER (w), 0) r_cumcnt_top_cid_{cid}'
        ]

        col_list = [c.split()[-1] for c in calc_list]
        col_list = [c.format(cid=cid) for c in col_list for cid in top_content_ids]
        create_list = [f'{c} INT64' for c in col_list + [f'ac_cumsum_pct_top_cid_{cid}' for cid in top_content_ids]]

        calc_list = [c.format(cid=cid) for c in calc_list for cid in top_content_ids]

        set_pct = 'ac_cumsum_pct_top_cid_{cid} = CAST(IFNULL(SAFE_DIVIDE(ac_cumsum_top_cid_{cid} * 100, r_cumcnt_top_cid_{cid}), -1) AS INT64)'

        set_list =[set_pct.format(cid=cid) for cid in top_content_ids]

        return f"""
        DROP TABLE IF EXISTS {self.DATASET}.{table_id};

        CREATE TABLE {self.DATASET}.{table_id} (
            row_id_tc INT64,
            {(',').join(create_list)}
        );

        INSERT INTO {self.DATASET}.{table_id}
            (row_id_tc, {(',').join(col_list)})
        SELECT
            row_id,
            {(',').join(calc_list)}
                  FROM {self.DATASET}.train
          WINDOW
            w AS (PARTITION BY user_id ORDER BY task_container_id_q
                RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING);

        UPDATE {self.DATASET}.{table_id}
        SET
            {(',').join(set_list)}
        WHERE true;
        """, sys._getframe().f_code.co_name + '_'


    # =================
    # ===== FOLDS =====
    # =================
    
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

    def update_folds(self, table_id='train', table_id_folds='folds'):
        return f"""
        UPDATE {self.DATASET}.{table_id} t
        SET t.fold = f.fold 
        FROM {self.DATASET}.{table_id_folds} f
        WHERE t.user_id = f.user_id_s
            AND t.task_container_id >= f.task_container_id_min
        """, sys._getframe().f_code.co_name + '_'

    
    # ============================
    # ===== LOCAL DATAFRAMES =====
    # ============================ 
    
    def select_train(self, columns=['*'], folds=[0],
                     excl_lectures=False, table_id='train',
                     table_id_folds='folds', limit=None, null_fold=False):

        folds = (' OR ').join([f'fold = {f}' for f  in folds])
        limit = f'LIMIT {limit}' if limit else ''
        excl_lectures = ' AND content_type_id = 0' if excl_lectures else ''
        null_fold = ' OR fold IS NULL' if null_fold else ''

        return f"""
            SELECT {(', ').join(columns)}
            FROM {self.DATASET}.{table_id} t
            LEFT JOIN {self.DATASET}.content_tags ct
            ON t.ql_id = ct.ql_id
            LEFT JOIN {self.DATASET}.roll_stats r
            ON t.row_id = r.row_id_r
            LEFT JOIN {self.DATASET}.top_content_ids tc
            ON t.row_id = tc.row_id_tc
            WHERE ({folds}{null_fold}){excl_lectures}            
            ORDER BY t.user_id, task_container_id, row_id
            {limit}
        """, sys._getframe().f_code.co_name + '_'
    
    def select_train_one_hots(self, columns=['*'], folds=[0],
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
            LEFT JOIN {self.DATASET}.one_hots o
            ON t.ql_id = o.ql_id
            WHERE ({folds}){excl_lectures}
            ORDER BY t.user_id, task_container_id, row_id
            {limit}
        """, sys._getframe().f_code.co_name + '_'
    
    def create_train_sample(self, table_id='train_sample', user_id_max=50000):
        return f"""
            CREATE TABLE {self.DATASET}.{table_id} AS
            SELECT *
            FROM {self.DATASET}.train
            WHERE user_id <= {user_id_max}
            ORDER BY user_id, task_container_id, row_id
        """, sys._getframe().f_code.co_name + '_'
    
    
    # ========================
    # ===== STATE TABLES =====
    # ========================
    
    def select_user_final_state(self, columns=None):
        return f"""
        WITH sorted_records AS (
          SELECT *, ROW_NUMBER() OVER (w) row_num
          FROM {self.DATASET}.train t
          JOIN {self.DATASET}.roll_stats r
          ON t.row_id = r.row_id_r
          JOIN {self.DATASET}.top_content_ids tc
          ON t.row_id = tc.row_id_tc 
          WINDOW
            w AS (PARTITION BY user_id ORDER BY row_id DESC)
        )
        SELECT {(', ').join(columns)}
        FROM sorted_records
        WHERE row_num = 1
        ORDER BY user_id
        """, sys._getframe().f_code.co_name + '_'    
    
    def select_users_content_final_state(self, table_id='train'):
        return f"""
        SELECT user_id, content_id,
            SUM(answered_correctly) ac_cumsum_content_id,
            SUM(CAST(content_type_id = 0 AS INT64)) r_cumcnt_content_id,
            IFNULL(CAST(AVG(pqet_current) AS INT64), -1) pqet_cumavg_content_id
        FROM {self.DATASET}.{table_id}
        GROUP BY user_id, content_id
        ORDER BY user_id, content_id
        """, sys._getframe().f_code.co_name + '_'
    
    def select_users_part_final_state(self, table_id='train'):
        return f"""
        SELECT user_id, part,
            SUM(answered_correctly) ac_cumsum_part,
            SUM(CAST(content_type_id = 0 AS INT64)) r_cumcnt_part,
            SUM(content_type_id) l_cumcnt_part,
            IFNULL(CAST(AVG(pqet_current) AS INT64), -1) pqet_cumavg_part
        FROM {self.DATASET}.{table_id} t
        JOIN {self.DATASET}.content_tags c
        ON t.ql_id = c.ql_id
        GROUP BY user_id, part
        ORDER BY user_id, part
        """, sys._getframe().f_code.co_name + '_'
    
    def select_users_tag_final_state(self, table_id='tag_response'):
        return f"""
        SELECT user_id, tag, SUM(answered_correctly) ac_cumsum_tag,
            SUM(CAST(content_type_id = 0 AS INT64)) r_cumcnt_tag,
            SUM(content_type_id) l_cumcnt_tag,
            IFNULL(CAST(AVG(pqet_current) AS INT64), -1) pqet_cumavg_tag
        FROM {self.DATASET}.{table_id}
        GROUP BY user_id, tag
        ORDER BY user_id, tag
        """, sys._getframe().f_code.co_name + '_'
    
    def select_users_part_tag_final_state(self, table_id='tag_response'):
        return f"""
        SELECT user_id, part, tag, SUM(answered_correctly) ac_cumsum_part_tag,
            SUM(CAST(content_type_id = 0 AS INT64)) r_cumcnt_part_tag,
            SUM(content_type_id) l_cumcnt_part_tag,
            IFNULL(CAST(AVG(pqet_current) AS INT64), -1) pqet_cumavg_part_tag
        FROM {self.DATASET}.{table_id}
        GROUP BY user_id, part, tag
        ORDER BY user_id, part, tag
        """, sys._getframe().f_code.co_name + '_'