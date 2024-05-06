import psycopg2
import argparse

parser = argparse.ArgumentParser(
    description='Define inputs for building database.')
parser.add_argument(
    '--database', type=str,
    help="database name")
parser.add_argument(
    '--username', type=str,
    help="username for database")
parser.add_argument(
    '--password', type=str,
    help="password for database")
parser.add_argument(
    '--host', type=str,
    help="host for database")
parser.add_argument(
    '--port', type=str,
    help="port for database")
parser.add_argument(
    '--table_name', type=str,
    help="table name in database")
parser.add_argument(
    '--schema_name', type=str)


if __name__ == '__main__':
    args = parser.parse_args()
    host = args.host
    database = args.database
    username = args.username
    password = args.password
    table_name = args.table_name
    schema_name = args.schema_name
    connection = psycopg2.connect(
                    host=host,
                    database=database,
                    user=username,
                    password=password)
    cursor = connection.cursor()
  #  UPDATE {schema_name}.{table_name} SET labels_predictions = labels_transformed_predictions;

    sql = """

    ALTER TABLE {schema_name}.{table_name}
    ADD COLUMN IF NOT EXISTS entryid TEXT GENERATED ALWAYS AS ("PatientID" || '-' || "StudyInstanceUID") STORED;



    CREATE TEMPORARY TABLE temp_table_0 AS
    SELECT * FROM {schema_name}.{table_name} WHERE
    ("Modality"='XA') ORDER BY entryid, "TimeStamp";
    CREATE INDEX IF NOT EXISTS index_entryid_temp_table_0
    ON temp_table_0(entryid);
    CREATE INDEX IF NOT EXISTS index_labels_predictions_temp_table_0
    ON temp_table_0(labels_predictions);
    CREATE INDEX IF NOT EXISTS index_timestamp_temp_table_0
    ON temp_table_0("TimeStamp");


    CREATE INDEX IF NOT EXISTS index_rowid_temp_table_0
    ON temp_table_0("rowid");
    CREATE TEMPORARY table test_table_rca AS SELECT * FROM temp_table_0 ORDER by entryid, "TimeStamp";
    alter table test_table_rca add rowid_2 serial;
    CREATE INDEX IF NOT EXISTS index_entryid_test_table
    ON test_table_rca(entryid);


    CREATE INDEX IF NOT EXISTS index_labels_predictions_test_table_rca ON test_table_rca(labels_predictions);
    CREATE INDEX IF NOT EXISTS index_timestamp_test_table
    ON test_table_rca("TimeStamp");
    CREATE INDEX IF NOT EXISTS index_rowid_test_table
    ON test_table_rca("rowid");
    CREATE INDEX IF NOT EXISTS index_rowid2_test_table
    ON test_table_rca("rowid_2");


     -- works until here

    CREATE TEMPORARY table rank_table AS
    select rr.entryid, rr."TimeStamp", rr.labels_predictions, rr.rowid, rr.rowid_2, rr.rank from (
    select tt.entryid, tt."TimeStamp", tt.labels_predictions, tt.rowid, tt.rowid_2, rank() over (partition by entryid order by "TimeStamp") rank
    from test_table_rca tt) rr;
    CREATE INDEX IF NOT EXISTS index_entryid_rank_table
    ON rank_table(entryid);
    CREATE INDEX IF NOT EXISTS index_labels_predictions_rank_table
    ON rank_table(labels_predictions);



    CREATE INDEX IF NOT EXISTS index_timestamp_rank_table
    ON rank_table("TimeStamp");
    CREATE INDEX IF NOT EXISTS index_rowid_rank_table
    ON rank_table("rowid");
    CREATE INDEX IF NOT EXISTS index_rowid2_rank_table
    ON rank_table("rowid_2");
    CREATE INDEX IF NOT EXISTS index_rank_rank_table
    ON rank_table("rank");



    CREATE TEMPORARY table min_rank_table AS SELECT m.entryid, m.rowid, m.rowid_2, m."TimeStamp", m.labels_predictions, t.min FROM (
    SELECT entryid, MIN(rank) AS min
    FROM rank_table
    where labels_predictions=1
    GROUP BY entryid
    ) t JOIN rank_table m ON m.entryid = t.entryid AND t.min = m.rank;

    CREATE INDEX IF NOT EXISTS index_entryid_min_rank_table
    ON min_rank_table(entryid);
    CREATE INDEX IF NOT EXISTS index_labels_predictions_min_rank_table ON min_rank_table(labels_predictions);
    CREATE INDEX IF NOT EXISTS index_timestamp_min_rank_table
    ON min_rank_table("TimeStamp");
    CREATE INDEX IF NOT EXISTS index_rowid_min_rank_table
    ON min_rank_table("rowid");


    CREATE INDEX IF NOT EXISTS index_rowid2_min_rank_table ON min_rank_table("rowid_2");
    CREATE INDEX IF NOT EXISTS index_min_min_rank_table ON min_rank_table ("min");
    -- break
    CREATE TEMPORARY table max_rank_table AS SELECT m.entryid, m.rowid, m.rowid_2, m."TimeStamp", m.labels_predictions, t.max FROM (
    SELECT entryid, Max(rank) AS max
    FROM rank_table
    where labels_predictions=1
    GROUP BY entryid
    ) t JOIN rank_table m ON m.entryid = t.entryid AND t.max = m.rank;


    CREATE INDEX IF NOT EXISTS index_entryid_max_rank_table ON max_rank_table(entryid);
    CREATE INDEX IF NOT EXISTS index_labels_predictions_max_rank_table ON max_rank_table(labels_predictions);
    CREATE INDEX IF NOT EXISTS index_timestamp_max_rank_table ON max_rank_table("TimeStamp");
    CREATE INDEX IF NOT EXISTS index_rowid_max_rank_table
    ON max_rank_table("rowid");
    CREATE INDEX IF NOT EXISTS index_rowid2_max_rank_table ON max_rank_table("rowid_2");
    CREATE INDEX IF NOT EXISTS index_min_max_rank_table
    ON max_rank_table ("max");



    CREATE TEMPORARY TABLE postive_candidate_table AS
    SELECT generate_series(min_t.rowid_2, max_t.rowid_2) FROM
    max_rank_table max_t JOIN min_rank_table min_t ON max_t.entryid = min_t.entryid; CREATE INDEX IF NOT EXISTS index_min_postive_candidate_table
    ON postive_candidate_table("generate_series");


    CREATE TEMPORARY TABLE postive_candidate_table_vl AS
    SELECT * FROM test_table_rca WHERE rowid_2 IN (SELECT * FROM postive_candidate_table) ; CREATE INDEX IF NOT EXISTS index_entryid_postive_candidate_table_vl
    ON postive_candidate_table_vl(entryid);



    CREATE INDEX IF NOT EXISTS index_labels_predictions_postive_candidate_table_vl ON postive_candidate_table_vl(labels_predictions);
    CREATE INDEX IF NOT EXISTS index_timestamp_postive_candidate_table_vl
    ON postive_candidate_table_vl("TimeStamp");
    CREATE INDEX IF NOT EXISTS index_rowid_postive_candidate_table_vl
    ON postive_candidate_table_vl("rowid");
    CREATE INDEX IF NOT EXISTS index_rowid2_postive_candidate_table_vl
    ON postive_candidate_table_vl("rowid_2");


    CREATE TEMPORARY table negative_table AS
    SELECT * FROM postive_candidate_table_vl ORDER by entryid, "TimeStamp"; alter table negative_table add rowid_3 serial;
    CREATE INDEX IF NOT EXISTS index_entryid_negative_table
    ON negative_table(entryid);
    CREATE INDEX IF NOT EXISTS index_labels_predictions_negative_table ON negative_table(labels_predictions);
    CREATE INDEX IF NOT EXISTS index_timestamp_negative_table
    ON negative_table("TimeStamp");


    CREATE INDEX IF NOT EXISTS index_rowid_negative_table
    ON negative_table("rowid");
    CREATE INDEX IF NOT EXISTS index_rowid2_negative_table ON negative_table("rowid_2");
    CREATE INDEX IF NOT EXISTS index_rowid3_negative_table ON negative_table("rowid_3");

    CREATE TEMPORARY table rank_table_2 AS
    select rr.entryid, rr."TimeStamp", rr.labels_predictions, rr.rowid, rr.rowid_3, rr.rank_2 from (
    select tt.entryid, tt."TimeStamp", tt.labels_predictions, tt.rowid, tt.rowid_3, rank() over (partition by entryid order by "TimeStamp") rank_2
    from negative_table tt) rr;
    CREATE INDEX IF NOT EXISTS index_entryid_rank_table_2
    ON rank_table_2(entryid);
    CREATE INDEX IF NOT EXISTS index_labels_predictions_rank_table_2
    ON rank_table_2(labels_predictions);


    CREATE INDEX IF NOT EXISTS index_timestamp_rank_table_2
    ON rank_table_2("TimeStamp");
    CREATE INDEX IF NOT EXISTS index_rowid_rank_table_2
    ON rank_table_2("rowid");
    CREATE INDEX IF NOT EXISTS index_rowid3_rank_table_2
    ON rank_table_2("rowid_3");
    CREATE INDEX IF NOT EXISTS index_rank2_rank_table_2
    ON rank_table_2("rank_2");
    -----BREAK




    -- probems here
    CREATE TEMPORARY table min_rank_table_2 AS SELECT m.entryid, m.rowid, m.rowid_3, m."TimeStamp", m.labels_predictions, t.min FROM (
    SELECT entryid, MIN(rank_2) AS min
    FROM rank_table_2
    where labels_predictions!=1
    GROUP BY entryid
    ) t JOIN rank_table_2 m ON m.entryid = t.entryid AND t.min = m.rank_2;




    CREATE INDEX IF NOT EXISTS index_entryid_min_rank_table_2
    ON min_rank_table_2(entryid);
    CREATE INDEX IF NOT EXISTS index_labels_predictions_min_rank_table_2 ON min_rank_table_2(labels_predictions);
    CREATE INDEX IF NOT EXISTS index_timestamp_min_rank_table_2
    ON min_rank_table_2("TimeStamp");
    CREATE INDEX IF NOT EXISTS index_rowid_min_rank_table_2
    ON min_rank_table_2("rowid");
    CREATE INDEX IF NOT EXISTS index_rowid3_min_rank_table_2
    ON min_rank_table_2("rowid_3");
    CREATE INDEX IF NOT EXISTS index_min_min_rank_table_2
    ON min_rank_table_2("min");



    CREATE TEMPORARY table max_rank_table_2 AS SELECT m.entryid, m.rowid, m.rowid_3, m."TimeStamp", m.labels_predictions, t.max FROM (
    SELECT entryid, MAX(rank_2) AS max
    FROM rank_table_2
    GROUP BY entryid ) t JOIN rank_table_2 m ON m.entryid = t.entryid AND t.max = m.rank_2;

    CREATE INDEX IF NOT EXISTS index_entryid_max_rank_table_2 ON max_rank_table_2(entryid);

    CREATE INDEX IF NOT EXISTS index_labels_predictions_max_rank_table_2
    ON max_rank_table_2(labels_predictions); CREATE INDEX IF NOT EXISTS index_timestamp_max_rank_table_2
    ON max_rank_table_2("TimeStamp");


    CREATE INDEX IF NOT EXISTS index_rowid_max_rank_table_2 ON max_rank_table_2("rowid");
    CREATE INDEX IF NOT EXISTS index_rowid3_max_rank_table_2 ON max_rank_table_2("rowid_3");
    CREATE INDEX IF NOT EXISTS index_max_max_rank_table_2
    ON max_rank_table_2("max");


    CREATE TEMPORARY TABLE postive_candidate_table_2 AS
    SELECT generate_series(min_t.rowid_3, max_t.rowid_3) FROM
    max_rank_table_2 max_t JOIN min_rank_table_2 min_t ON max_t.entryid = min_t.entryid; CREATE INDEX IF NOT EXISTS index_min_postive_candidate_table_2
    ON postive_candidate_table_2("generate_series");

    CREATE TEMPORARY TABLE negative_table_2 AS SELECT * FROM negative_table WHERE rowid_3 IN (SELECT * FROM postive_candidate_table_2);

    CREATE INDEX IF NOT EXISTS index_rowid3_negative_table_2 ON negative_table_2("rowid_3");

    CREATE TEMPORARY TABLE "test_deploy_rca" AS SELECT * FROM negative_table WHERE (rowid_3 NOT IN (SELECT * FROM postive_candidate_table_2)) ORDER BY "rowid", "PatientID";



     ------------------------------------- STARTING LCA


    CREATE TEMPORARY TABLE temp_table_0_lca AS
    SELECT * FROM {schema_name}.{table_name} WHERE
    ("Modality"='XA') ORDER BY entryid, "TimeStamp";
    CREATE INDEX IF NOT EXISTS index_entryid_temp_table_0_lca
    ON temp_table_0_lca(entryid);
    CREATE INDEX IF NOT EXISTS index_labels_predictions_temp_table_0_lca
    ON temp_table_0_lca(labels_predictions);
    CREATE INDEX IF NOT EXISTS index_timestamp_temp_table_0_lca
    ON temp_table_0_lca("TimeStamp");


    CREATE INDEX IF NOT EXISTS index_rowid_temp_table_0_lca
    ON temp_table_0_lca("rowid");
    CREATE TEMPORARY table test_table_lca AS SELECT * FROM temp_table_0_lca ORDER by entryid, "TimeStamp";
    alter table test_table_lca add rowid_2 serial;
    CREATE INDEX IF NOT EXISTS index_entryid_test_table_lca
    ON test_table_lca(entryid);


    CREATE INDEX IF NOT EXISTS index_labels_predictions_test_table_lca ON test_table_lca(labels_predictions);
    CREATE INDEX IF NOT EXISTS index_timestamp_test_table_lca
    ON test_table_lca("TimeStamp");
    CREATE INDEX IF NOT EXISTS index_rowid_test_table_lca
    ON test_table_lca("rowid");
    CREATE INDEX IF NOT EXISTS index_rowid2_test_table_lca
    ON test_table_lca("rowid_2");


    -- works until here

    CREATE TEMPORARY table rank_table_lca AS
    select rr.entryid, rr."TimeStamp", rr.labels_predictions, rr.rowid, rr.rowid_2, rr.rank from (
    select tt.entryid, tt."TimeStamp", tt.labels_predictions, tt.rowid, tt.rowid_2, rank() over (partition by entryid order by "TimeStamp") rank
    from test_table_lca tt) rr;
    CREATE INDEX IF NOT EXISTS index_entryid_rank_table_lca
    ON rank_table_lca(entryid);
    CREATE INDEX IF NOT EXISTS index_labels_predictions_rank_table_lca
    ON rank_table_lca(labels_predictions);


    CREATE INDEX IF NOT EXISTS index_timestamp_rank_table_lca
    ON rank_table_lca("TimeStamp");
    CREATE INDEX IF NOT EXISTS index_rowid_rank_table_lca
    ON rank_table_lca("rowid");
    CREATE INDEX IF NOT EXISTS index_rowid2_rank_table_lca
    ON rank_table_lca("rowid_2");
    CREATE INDEX IF NOT EXISTS index_rank_rank_table_lca
    ON rank_table_lca("rank");



    CREATE TEMPORARY table min_rank_table_lca AS SELECT m.entryid, m.rowid, m.rowid_2, m."TimeStamp", m.labels_predictions, t.min FROM (
    SELECT entryid, MIN(rank) AS min
    FROM rank_table_lca
    where labels_predictions=0
    GROUP BY entryid
    ) t JOIN rank_table_lca m ON m.entryid = t.entryid AND t.min = m.rank;

    CREATE INDEX IF NOT EXISTS index_entryid_min_rank_table_lca
    ON min_rank_table_lca(entryid);
    CREATE INDEX IF NOT EXISTS index_labels_predictions_min_rank_table_lca ON min_rank_table_lca(labels_predictions);
    CREATE INDEX IF NOT EXISTS index_timestamp_min_rank_table_lca
    ON min_rank_table_lca("TimeStamp");
    CREATE INDEX IF NOT EXISTS index_rowid_min_rank_table_lca
    ON min_rank_table_lca("rowid");


    CREATE INDEX IF NOT EXISTS index_rowid2_min_rank_table_lca ON min_rank_table_lca("rowid_2");
    CREATE INDEX IF NOT EXISTS index_min_min_rank_table_lca ON min_rank_table_lca ("min");
    -- break
    CREATE TEMPORARY table max_rank_table_lca AS SELECT m.entryid, m.rowid, m.rowid_2, m."TimeStamp", m.labels_predictions, t.max FROM (
    SELECT entryid, Max(rank) AS max
    FROM rank_table_lca
    where labels_predictions=0
    GROUP BY entryid
    ) t JOIN rank_table_lca m ON m.entryid = t.entryid AND t.max = m.rank;


    CREATE INDEX IF NOT EXISTS index_entryid_max_rank_table_lca ON max_rank_table_lca(entryid);
    CREATE INDEX IF NOT EXISTS index_labels_predictions_max_rank_table_lca ON max_rank_table_lca(labels_predictions);
    CREATE INDEX IF NOT EXISTS index_timestamp_max_rank_table_lca ON max_rank_table_lca("TimeStamp");
    CREATE INDEX IF NOT EXISTS index_rowid_max_rank_table_lca
    ON max_rank_table_lca("rowid");
    CREATE INDEX IF NOT EXISTS index_rowid2_max_rank_table_lca ON max_rank_table_lca("rowid_2");
    CREATE INDEX IF NOT EXISTS index_min_max_rank_table_lca
    ON max_rank_table_lca ("max");



    CREATE TEMPORARY TABLE postive_candidate_table_lca AS
    SELECT generate_series(min_t.rowid_2, max_t.rowid_2) FROM
    max_rank_table_lca max_t JOIN min_rank_table_lca min_t ON max_t.entryid = min_t.entryid; CREATE INDEX IF NOT EXISTS index_min_postive_candidate_table_lca
    ON postive_candidate_table_lca("generate_series");


    CREATE TEMPORARY TABLE postive_candidate_table_lca_vl AS
    SELECT * FROM test_table_lca WHERE rowid_2 IN (SELECT * FROM postive_candidate_table_lca) ; CREATE INDEX IF NOT EXISTS index_entryid_postive_candidate_table_lca_vl
    ON postive_candidate_table_lca_vl(entryid);



    CREATE INDEX IF NOT EXISTS index_labels_predictions_postive_candidate_table_lca_vl ON postive_candidate_table_lca_vl(labels_predictions);
    CREATE INDEX IF NOT EXISTS index_timestamp_postive_candidate_table_lca_vl
    ON postive_candidate_table_lca_vl("TimeStamp");
    CREATE INDEX IF NOT EXISTS index_rowid_postive_candidate_table_lca_vl
    ON postive_candidate_table_lca_vl("rowid");
    CREATE INDEX IF NOT EXISTS index_rowid2_postive_candidate_table_lca_vl
    ON postive_candidate_table_lca_vl("rowid_2");


    CREATE TEMPORARY table negative_table_lca AS
    SELECT * FROM postive_candidate_table_lca_vl ORDER by entryid, "TimeStamp"; alter table negative_table_lca add rowid_3 serial;
    CREATE INDEX IF NOT EXISTS index_entryid_negative_table_lca
    ON negative_table_lca(entryid);
    CREATE INDEX IF NOT EXISTS index_labels_predictions_negative_table_lca ON negative_table_lca(labels_predictions);
    CREATE INDEX IF NOT EXISTS index_timestamp_negative_table_lca
    ON negative_table_lca("TimeStamp");


    CREATE INDEX IF NOT EXISTS index_rowid_negative_table_lca
    ON negative_table_lca("rowid");
    CREATE INDEX IF NOT EXISTS index_rowid2_negative_table_lca ON negative_table_lca("rowid_2");
    CREATE INDEX IF NOT EXISTS index_rowid3_negative_table_lca ON negative_table_lca("rowid_3");

    CREATE TEMPORARY table rank_table_lca_2 AS
    select rr.entryid, rr."TimeStamp", rr.labels_predictions, rr.rowid, rr.rowid_3, rr.rank_2 from (
    select tt.entryid, tt."TimeStamp", tt.labels_predictions, tt.rowid, tt.rowid_3, rank() over (partition by entryid order by "TimeStamp") rank_2
    from negative_table_lca tt) rr;
    CREATE INDEX IF NOT EXISTS index_entryid_rank_table_lca_2
    ON rank_table_lca_2(entryid);
    CREATE INDEX IF NOT EXISTS index_labels_predictions_rank_table_lca_2
    ON rank_table_lca_2(labels_predictions);


    CREATE INDEX IF NOT EXISTS index_timestamp_rank_table_lca_2
    ON rank_table_lca_2("TimeStamp");
    CREATE INDEX IF NOT EXISTS index_rowid_rank_table_lca_2
    ON rank_table_lca_2("rowid");
    CREATE INDEX IF NOT EXISTS index_rowid3_rank_table_lca_2
    ON rank_table_lca_2("rowid_3");
    CREATE INDEX IF NOT EXISTS index_rank2_rank_table_lca_2
    ON rank_table_lca_2("rank_2");
    -----BREAK




    -- probems here
    CREATE TEMPORARY table min_rank_table_lca_2 AS SELECT m.entryid, m.rowid, m.rowid_3, m."TimeStamp", m.labels_predictions, t.min FROM (
    SELECT entryid, MIN(rank_2) AS min
    FROM rank_table_lca_2
    where labels_predictions!=0
    GROUP BY entryid
    ) t JOIN rank_table_lca_2 m ON m.entryid = t.entryid AND t.min = m.rank_2;




    CREATE INDEX IF NOT EXISTS index_entryid_min_rank_table_lca_2
    ON min_rank_table_lca_2(entryid);
    CREATE INDEX IF NOT EXISTS index_labels_predictions_min_rank_table_lca_2 ON min_rank_table_lca_2(labels_predictions);
    CREATE INDEX IF NOT EXISTS index_timestamp_min_rank_table_lca_2
    ON min_rank_table_lca_2("TimeStamp");
    CREATE INDEX IF NOT EXISTS index_rowid_min_rank_table_lca_2
    ON min_rank_table_lca_2("rowid");
    CREATE INDEX IF NOT EXISTS index_rowid3_min_rank_table_lca_2
    ON min_rank_table_lca_2("rowid_3");
    CREATE INDEX IF NOT EXISTS index_min_min_rank_table_lca_2
    ON min_rank_table_lca_2("min");



    CREATE TEMPORARY table max_rank_table_lca_2 AS SELECT m.entryid, m.rowid, m.rowid_3, m."TimeStamp", m.labels_predictions, t.max FROM (
    SELECT entryid, MAX(rank_2) AS max
    FROM rank_table_lca_2
    GROUP BY entryid ) t JOIN rank_table_lca_2 m ON m.entryid = t.entryid AND t.max = m.rank_2;

    CREATE INDEX IF NOT EXISTS index_entryid_max_rank_table_lca_2 ON max_rank_table_lca_2(entryid);

    CREATE INDEX IF NOT EXISTS index_labels_predictions_max_rank_table_lca_2
    ON max_rank_table_lca_2(labels_predictions); CREATE INDEX IF NOT EXISTS index_timestamp_max_rank_table_lca_2
    ON max_rank_table_lca_2("TimeStamp");


    CREATE INDEX IF NOT EXISTS index_rowid_max_rank_table_lca_2 ON max_rank_table_lca_2("rowid");
    CREATE INDEX IF NOT EXISTS index_rowid3_max_rank_table_lca_2 ON max_rank_table_lca_2("rowid_3");
    CREATE INDEX IF NOT EXISTS index_max_max_rank_table_lca_2
    ON max_rank_table_lca_2("max");


    CREATE TEMPORARY TABLE postive_candidate_table_2_lca AS
    SELECT generate_series(min_t.rowid_3, max_t.rowid_3) FROM
    max_rank_table_lca_2 max_t JOIN min_rank_table_lca_2 min_t ON max_t.entryid = min_t.entryid; CREATE INDEX IF NOT EXISTS index_min_postive_candidate_table_2_lca
    ON postive_candidate_table_2_lca("generate_series");

    CREATE TEMPORARY TABLE negative_table_lca_2 AS SELECT * FROM negative_table_lca WHERE rowid_3 IN (SELECT * FROM postive_candidate_table_2_lca);

    CREATE INDEX IF NOT EXISTS index_rowid3_negative_table_lca_2 ON negative_table_lca_2("rowid_3");

    CREATE TABLE "test_deploy_lca" AS SELECT * FROM negative_table_lca WHERE (rowid_3 NOT IN (SELECT * FROM postive_candidate_table_2_lca)) ORDER BY "rowid", "PatientID";


    CREATE TEMPORARY TABLE diagnostic_data AS
    SELECT * FROM "test_deploy_rca" 
    UNION
    SELECT * FROM "test_deploy_lca";



    UPDATE {schema_name}.{table_name} SET diagnostic_data = TRUE WHERE rowid IN (SELECT rowid FROM diagnostic_data);
    """.format(schema_name=schema_name, table_name=table_name)
    cursor.execute(sql)
    cursor.execute("COMMIT;")
    cursor.close()
    connection.close()

    ## GRAVEYARD
    # ALTER TABLE cag.test_table
    # ADD COLUMN entryid TEXT GENERATED ALWAYS AS ("PatientID" || '-' || "StudyInstanceUID") STORED;




    # CREATE TEMPORARY TABLE temp_table_0 AS
    # SELECT * FROM cag."test_table" WHERE
    # ("Modality"='XA') ORDER BY entryid, "TimeStamp";
    # CREATE INDEX IF NOT EXISTS index_entryid_temp_table_0
    # ON temp_table_0(entryid);
    # CREATE INDEX IF NOT EXISTS index_labels_predictions_temp_table_0
    # ON temp_table_0(labels_predictions);
    # CREATE INDEX IF NOT EXISTS index_timestamp_temp_table_0
    # ON temp_table_0("TimeStamp");


    # CREATE INDEX IF NOT EXISTS index_rowid_temp_table_0
    # ON temp_table_0("rowid");
    # CREATE TEMPORARY table test_table AS SELECT * FROM temp_table_0 ORDER by entryid, "TimeStamp";
    # alter table test_table add rowid_2 serial;
    # CREATE INDEX IF NOT EXISTS index_entryid_test_table
    # ON test_table(entryid);


    # CREATE INDEX IF NOT EXISTS index_labels_predictions_test_table ON test_table(labels_predictions);
    # CREATE INDEX IF NOT EXISTS index_timestamp_test_table
    # ON test_table("TimeStamp");
    # CREATE INDEX IF NOT EXISTS index_rowid_test_table
    # ON test_table("rowid");
    # CREATE INDEX IF NOT EXISTS index_rowid2_test_table
    # ON test_table("rowid_2");


    # -- works until here

    # CREATE TEMPORARY table rank_table AS
    # select rr.entryid, rr."TimeStamp", rr.labels_predictions, rr.rowid, rr.rowid_2, rr.rank from (
    # select tt.entryid, tt."TimeStamp", tt.labels_predictions, tt.rowid, tt.rowid_2, rank() over (partition by entryid order by "TimeStamp") rank
    # from test_table tt) rr;
    # CREATE INDEX IF NOT EXISTS index_entryid_rank_table
    # ON rank_table(entryid);
    # CREATE INDEX IF NOT EXISTS index_labels_predictions_rank_table
    # ON rank_table(labels_predictions);


    # CREATE INDEX IF NOT EXISTS index_timestamp_rank_table
    # ON rank_table("TimeStamp");
    # CREATE INDEX IF NOT EXISTS index_rowid_rank_table
    # ON rank_table("rowid");
    # CREATE INDEX IF NOT EXISTS index_rowid2_rank_table
    # ON rank_table("rowid_2");
    # CREATE INDEX IF NOT EXISTS index_rank_rank_table
    # ON rank_table("rank");



    # CREATE TEMPORARY table min_rank_table AS SELECT m.entryid, m.rowid, m.rowid_2, m."TimeStamp", m.labels_predictions, t.min FROM (
    # SELECT entryid, MIN(rank) AS min
    # FROM rank_table
    # where labels_predictions=1
    # GROUP BY entryid
    # ) t JOIN rank_table m ON m.entryid = t.entryid AND t.min = m.rank;

    # CREATE INDEX IF NOT EXISTS index_entryid_min_rank_table
    # ON min_rank_table(entryid);
    # CREATE INDEX IF NOT EXISTS index_labels_predictions_min_rank_table ON min_rank_table(labels_predictions);
    # CREATE INDEX IF NOT EXISTS index_timestamp_min_rank_table
    # ON min_rank_table("TimeStamp");
    # CREATE INDEX IF NOT EXISTS index_rowid_min_rank_table
    # ON min_rank_table("rowid");


    # CREATE INDEX IF NOT EXISTS index_rowid2_min_rank_table ON min_rank_table("rowid_2");
    # CREATE INDEX IF NOT EXISTS index_min_min_rank_table ON min_rank_table ("min");
    # -- break
    # CREATE TEMPORARY table max_rank_table AS SELECT m.entryid, m.rowid, m.rowid_2, m."TimeStamp", m.labels_predictions, t.max FROM (
    # SELECT entryid, Max(rank) AS max
    # FROM rank_table
    # where labels_predictions=1
    # GROUP BY entryid
    # ) t JOIN rank_table m ON m.entryid = t.entryid AND t.max = m.rank;


    # CREATE INDEX IF NOT EXISTS index_entryid_max_rank_table ON max_rank_table(entryid);
    # CREATE INDEX IF NOT EXISTS index_labels_predictions_max_rank_table ON max_rank_table(labels_predictions);
    # CREATE INDEX IF NOT EXISTS index_timestamp_max_rank_table ON max_rank_table("TimeStamp");
    # CREATE INDEX IF NOT EXISTS index_rowid_max_rank_table
    # ON max_rank_table("rowid");
    # CREATE INDEX IF NOT EXISTS index_rowid2_max_rank_table ON max_rank_table("rowid_2");
    # CREATE INDEX IF NOT EXISTS index_min_max_rank_table
    # ON max_rank_table ("max");



    # CREATE TEMPORARY TABLE postive_candidate_table AS
    # SELECT generate_series(min_t.rowid_2, max_t.rowid_2) FROM
    # max_rank_table max_t JOIN min_rank_table min_t ON max_t.entryid = min_t.entryid; CREATE INDEX IF NOT EXISTS index_min_postive_candidate_table
    # ON postive_candidate_table("generate_series");


    # CREATE TEMPORARY TABLE postive_candidate_table_vl AS
    # SELECT * FROM test_table WHERE rowid_2 IN (SELECT * FROM postive_candidate_table) ; CREATE INDEX IF NOT EXISTS index_entryid_postive_candidate_table_vl
    # ON postive_candidate_table_vl(entryid);



    # CREATE INDEX IF NOT EXISTS index_labels_predictions_postive_candidate_table_vl ON postive_candidate_table_vl(labels_predictions);
    # CREATE INDEX IF NOT EXISTS index_timestamp_postive_candidate_table_vl
    # ON postive_candidate_table_vl("TimeStamp");
    # CREATE INDEX IF NOT EXISTS index_rowid_postive_candidate_table_vl
    # ON postive_candidate_table_vl("rowid");
    # CREATE INDEX IF NOT EXISTS index_rowid2_postive_candidate_table_vl
    # ON postive_candidate_table_vl("rowid_2");


    # CREATE TEMPORARY table negative_table AS
    # SELECT * FROM postive_candidate_table_vl ORDER by entryid, "TimeStamp"; alter table negative_table add rowid_3 serial;
    # CREATE INDEX IF NOT EXISTS index_entryid_negative_table
    # ON negative_table(entryid);
    # CREATE INDEX IF NOT EXISTS index_labels_predictions_negative_table ON negative_table(labels_predictions);
    # CREATE INDEX IF NOT EXISTS index_timestamp_negative_table
    # ON negative_table("TimeStamp");


    # CREATE INDEX IF NOT EXISTS index_rowid_negative_table
    # ON negative_table("rowid");
    # CREATE INDEX IF NOT EXISTS index_rowid2_negative_table ON negative_table("rowid_2");
    # CREATE INDEX IF NOT EXISTS index_rowid3_negative_table ON negative_table("rowid_3");

    # CREATE TEMPORARY table rank_table_2 AS
    # select rr.entryid, rr."TimeStamp", rr.labels_predictions, rr.rowid, rr.rowid_3, rr.rank_2 from (
    # select tt.entryid, tt."TimeStamp", tt.labels_predictions, tt.rowid, tt.rowid_3, rank() over (partition by entryid order by "TimeStamp") rank_2
    # from negative_table tt) rr;
    # CREATE INDEX IF NOT EXISTS index_entryid_rank_table_2
    # ON rank_table_2(entryid);
    # CREATE INDEX IF NOT EXISTS index_labels_predictions_rank_table_2
    # ON rank_table_2(labels_predictions);


    # CREATE INDEX IF NOT EXISTS index_timestamp_rank_table_2
    # ON rank_table_2("TimeStamp");
    # CREATE INDEX IF NOT EXISTS index_rowid_rank_table_2
    # ON rank_table_2("rowid");
    # CREATE INDEX IF NOT EXISTS index_rowid3_rank_table_2
    # ON rank_table_2("rowid_3");
    # CREATE INDEX IF NOT EXISTS index_rank2_rank_table_2
    # ON rank_table_2("rank_2");
    # -----BREAK




    # -- probems here
    # CREATE TEMPORARY table min_rank_table_2 AS SELECT m.entryid, m.rowid, m.rowid_3, m."TimeStamp", m.labels_predictions, t.min FROM (
    # SELECT entryid, MIN(rank_2) AS min
    # FROM rank_table_2
    # where labels_predictions!=1
    # GROUP BY entryid
    # ) t JOIN rank_table_2 m ON m.entryid = t.entryid AND t.min = m.rank_2;




    # CREATE INDEX IF NOT EXISTS index_entryid_min_rank_table_2
    # ON min_rank_table_2(entryid);
    # CREATE INDEX IF NOT EXISTS index_labels_predictions_min_rank_table_2 ON min_rank_table_2(labels_predictions);
    # CREATE INDEX IF NOT EXISTS index_timestamp_min_rank_table_2
    # ON min_rank_table_2("TimeStamp");
    # CREATE INDEX IF NOT EXISTS index_rowid_min_rank_table_2
    # ON min_rank_table_2("rowid");
    # CREATE INDEX IF NOT EXISTS index_rowid3_min_rank_table_2
    # ON min_rank_table_2("rowid_3");
    # CREATE INDEX IF NOT EXISTS index_min_min_rank_table_2
    # ON min_rank_table_2("min");



    # CREATE TEMPORARY table max_rank_table_2 AS SELECT m.entryid, m.rowid, m.rowid_3, m."TimeStamp", m.labels_predictions, t.max FROM (
    # SELECT entryid, MAX(rank_2) AS max
    # FROM rank_table_2
    # GROUP BY entryid ) t JOIN rank_table_2 m ON m.entryid = t.entryid AND t.max = m.rank_2;

    # CREATE INDEX IF NOT EXISTS index_entryid_max_rank_table_2 ON max_rank_table_2(entryid);

    # CREATE INDEX IF NOT EXISTS index_labels_predictions_max_rank_table_2
    # ON max_rank_table_2(labels_predictions); CREATE INDEX IF NOT EXISTS index_timestamp_max_rank_table_2
    # ON max_rank_table_2("TimeStamp");


    # CREATE INDEX IF NOT EXISTS index_rowid_max_rank_table_2 ON max_rank_table_2("rowid");
    # CREATE INDEX IF NOT EXISTS index_rowid3_max_rank_table_2 ON max_rank_table_2("rowid_3");
    # CREATE INDEX IF NOT EXISTS index_max_max_rank_table_2
    # ON max_rank_table_2("max");


    # CREATE TEMPORARY TABLE postive_candidate_table_2 AS
    # SELECT generate_series(min_t.rowid_3, max_t.rowid_3) FROM
    # max_rank_table_2 max_t JOIN min_rank_table_2 min_t ON max_t.entryid = min_t.entryid; CREATE INDEX IF NOT EXISTS index_min_postive_candidate_table_2
    # ON postive_candidate_table_2("generate_series");

    # CREATE TEMPORARY TABLE negative_table_2 AS SELECT * FROM negative_table WHERE rowid_3 IN (SELECT * FROM postive_candidate_table_2);

    # CREATE INDEX IF NOT EXISTS index_rowid3_negative_table_2 ON negative_table_2("rowid_3");

    # CREATE TEMPORARY TABLE cag."test_deploy_rca" AS SELECT * FROM negative_table WHERE (rowid_3 NOT IN (SELECT * FROM postive_candidate_table_2)) ORDER BY "rowid", "PatientID";


    # ------------------------------------- STARTING LCA


    # CREATE TEMPORARY TABLE temp_table_0_lca AS
    # SELECT * FROM cag."test_table" WHERE
    # ("Modality"='XA') ORDER BY entryid, "TimeStamp";
    # CREATE INDEX IF NOT EXISTS index_entryid_temp_table_0_lca
    # ON temp_table_0_lca(entryid);
    # CREATE INDEX IF NOT EXISTS index_labels_predictions_temp_table_0_lca
    # ON temp_table_0_lca(labels_predictions);
    # CREATE INDEX IF NOT EXISTS index_timestamp_temp_table_0_lca
    # ON temp_table_0_lca("TimeStamp");


    # CREATE INDEX IF NOT EXISTS index_rowid_temp_table_0_lca
    # ON temp_table_0_lca("rowid");
    # CREATE TEMPORARY table test_table_lca AS SELECT * FROM temp_table_0_lca ORDER by entryid, "TimeStamp";
    # alter table test_table_lca add rowid_2 serial;
    # CREATE INDEX IF NOT EXISTS index_entryid_test_table_lca
    # ON test_table_lca(entryid);


    # CREATE INDEX IF NOT EXISTS index_labels_predictions_test_table_lca ON test_table_lca(labels_predictions);
    # CREATE INDEX IF NOT EXISTS index_timestamp_test_table_lca
    # ON test_table_lca("TimeStamp");
    # CREATE INDEX IF NOT EXISTS index_rowid_test_table_lca
    # ON test_table_lca("rowid");
    # CREATE INDEX IF NOT EXISTS index_rowid2_test_table_lca
    # ON test_table_lca("rowid_2");


    # -- works until here

    # CREATE TEMPORARY table rank_table_lca AS
    # select rr.entryid, rr."TimeStamp", rr.labels_predictions, rr.rowid, rr.rowid_2, rr.rank from (
    # select tt.entryid, tt."TimeStamp", tt.labels_predictions, tt.rowid, tt.rowid_2, rank() over (partition by entryid order by "TimeStamp") rank
    # from test_table_lca tt) rr;
    # CREATE INDEX IF NOT EXISTS index_entryid_rank_table_lca
    # ON rank_table_lca(entryid);
    # CREATE INDEX IF NOT EXISTS index_labels_predictions_rank_table_lca
    # ON rank_table_lca(labels_predictions);


    # CREATE INDEX IF NOT EXISTS index_timestamp_rank_table_lca
    # ON rank_table_lca("TimeStamp");
    # CREATE INDEX IF NOT EXISTS index_rowid_rank_table_lca
    # ON rank_table_lca("rowid");
    # CREATE INDEX IF NOT EXISTS index_rowid2_rank_table_lca
    # ON rank_table_lca("rowid_2");
    # CREATE INDEX IF NOT EXISTS index_rank_rank_table_lca
    # ON rank_table_lca("rank");



    # CREATE TEMPORARY table min_rank_table_lca AS SELECT m.entryid, m.rowid, m.rowid_2, m."TimeStamp", m.labels_predictions, t.min FROM (
    # SELECT entryid, MIN(rank) AS min
    # FROM rank_table_lca
    # where labels_predictions=0
    # GROUP BY entryid
    # ) t JOIN rank_table_lca m ON m.entryid = t.entryid AND t.min = m.rank;

    # CREATE INDEX IF NOT EXISTS index_entryid_min_rank_table_lca
    # ON min_rank_table_lca(entryid);
    # CREATE INDEX IF NOT EXISTS index_labels_predictions_min_rank_table_lca ON min_rank_table_lca(labels_predictions);
    # CREATE INDEX IF NOT EXISTS index_timestamp_min_rank_table_lca
    # ON min_rank_table_lca("TimeStamp");
    # CREATE INDEX IF NOT EXISTS index_rowid_min_rank_table_lca
    # ON min_rank_table_lca("rowid");


    # CREATE INDEX IF NOT EXISTS index_rowid2_min_rank_table_lca ON min_rank_table_lca("rowid_2");
    # CREATE INDEX IF NOT EXISTS index_min_min_rank_table_lca ON min_rank_table_lca ("min");
    # -- break
    # CREATE TEMPORARY table max_rank_table_lca AS SELECT m.entryid, m.rowid, m.rowid_2, m."TimeStamp", m.labels_predictions, t.max FROM (
    # SELECT entryid, Max(rank) AS max
    # FROM rank_table_lca
    # where labels_predictions=0
    # GROUP BY entryid
    # ) t JOIN rank_table_lca m ON m.entryid = t.entryid AND t.max = m.rank;


    # CREATE INDEX IF NOT EXISTS index_entryid_max_rank_table_lca ON max_rank_table_lca(entryid);
    # CREATE INDEX IF NOT EXISTS index_labels_predictions_max_rank_table_lca ON max_rank_table_lca(labels_predictions);
    # CREATE INDEX IF NOT EXISTS index_timestamp_max_rank_table_lca ON max_rank_table_lca("TimeStamp");
    # CREATE INDEX IF NOT EXISTS index_rowid_max_rank_table_lca
    # ON max_rank_table_lca("rowid");
    # CREATE INDEX IF NOT EXISTS index_rowid2_max_rank_table_lca ON max_rank_table_lca("rowid_2");
    # CREATE INDEX IF NOT EXISTS index_min_max_rank_table_lca
    # ON max_rank_table_lca ("max");



    # CREATE TEMPORARY TABLE postive_candidate_table_lca AS
    # SELECT generate_series(min_t.rowid_2, max_t.rowid_2) FROM
    # max_rank_table_lca max_t JOIN min_rank_table_lca min_t ON max_t.entryid = min_t.entryid; CREATE INDEX IF NOT EXISTS index_min_postive_candidate_table_lca
    # ON postive_candidate_table_lca("generate_series");


    # CREATE TEMPORARY TABLE postive_candidate_table_lca_vl AS
    # SELECT * FROM test_table_lca WHERE rowid_2 IN (SELECT * FROM postive_candidate_table_lca) ; CREATE INDEX IF NOT EXISTS index_entryid_postive_candidate_table_lca_vl
    # ON postive_candidate_table_lca_vl(entryid);



    # CREATE INDEX IF NOT EXISTS index_labels_predictions_postive_candidate_table_lca_vl ON postive_candidate_table_lca_vl(labels_predictions);
    # CREATE INDEX IF NOT EXISTS index_timestamp_postive_candidate_table_lca_vl
    # ON postive_candidate_table_lca_vl("TimeStamp");
    # CREATE INDEX IF NOT EXISTS index_rowid_postive_candidate_table_lca_vl
    # ON postive_candidate_table_lca_vl("rowid");
    # CREATE INDEX IF NOT EXISTS index_rowid2_postive_candidate_table_lca_vl
    # ON postive_candidate_table_lca_vl("rowid_2");


    # CREATE TEMPORARY table negative_table_lca AS
    # SELECT * FROM postive_candidate_table_lca_vl ORDER by entryid, "TimeStamp"; alter table negative_table_lca add rowid_3 serial;
    # CREATE INDEX IF NOT EXISTS index_entryid_negative_table_lca
    # ON negative_table_lca(entryid);
    # CREATE INDEX IF NOT EXISTS index_labels_predictions_negative_table_lca ON negative_table_lca(labels_predictions);
    # CREATE INDEX IF NOT EXISTS index_timestamp_negative_table_lca
    # ON negative_table_lca("TimeStamp");


    # CREATE INDEX IF NOT EXISTS index_rowid_negative_table_lca
    # ON negative_table_lca("rowid");
    # CREATE INDEX IF NOT EXISTS index_rowid2_negative_table_lca ON negative_table_lca("rowid_2");
    # CREATE INDEX IF NOT EXISTS index_rowid3_negative_table_lca ON negative_table_lca("rowid_3");

    # CREATE TEMPORARY table rank_table_lca_2 AS
    # select rr.entryid, rr."TimeStamp", rr.labels_predictions, rr.rowid, rr.rowid_3, rr.rank_2 from (
    # select tt.entryid, tt."TimeStamp", tt.labels_predictions, tt.rowid, tt.rowid_3, rank() over (partition by entryid order by "TimeStamp") rank_2
    # from negative_table_lca tt) rr;
    # CREATE INDEX IF NOT EXISTS index_entryid_rank_table_lca_2
    # ON rank_table_lca_2(entryid);
    # CREATE INDEX IF NOT EXISTS index_labels_predictions_rank_table_lca_2
    # ON rank_table_lca_2(labels_predictions);


    # CREATE INDEX IF NOT EXISTS index_timestamp_rank_table_lca_2
    # ON rank_table_lca_2("TimeStamp");
    # CREATE INDEX IF NOT EXISTS index_rowid_rank_table_lca_2
    # ON rank_table_lca_2("rowid");
    # CREATE INDEX IF NOT EXISTS index_rowid3_rank_table_lca_2
    # ON rank_table_lca_2("rowid_3");
    # CREATE INDEX IF NOT EXISTS index_rank2_rank_table_lca_2
    # ON rank_table_lca_2("rank_2");
    # -----BREAK




    # -- probems here
    # CREATE TEMPORARY table min_rank_table_lca_2 AS SELECT m.entryid, m.rowid, m.rowid_3, m."TimeStamp", m.labels_predictions, t.min FROM (
    # SELECT entryid, MIN(rank_2) AS min
    # FROM rank_table_lca_2
    # where labels_predictions!=0
    # GROUP BY entryid
    # ) t JOIN rank_table_lca_2 m ON m.entryid = t.entryid AND t.min = m.rank_2;




    # CREATE INDEX IF NOT EXISTS index_entryid_min_rank_table_lca_2
    # ON min_rank_table_lca_2(entryid);
    # CREATE INDEX IF NOT EXISTS index_labels_predictions_min_rank_table_lca_2 ON min_rank_table_lca_2(labels_predictions);
    # CREATE INDEX IF NOT EXISTS index_timestamp_min_rank_table_lca_2
    # ON min_rank_table_lca_2("TimeStamp");
    # CREATE INDEX IF NOT EXISTS index_rowid_min_rank_table_lca_2
    # ON min_rank_table_lca_2("rowid");
    # CREATE INDEX IF NOT EXISTS index_rowid3_min_rank_table_lca_2
    # ON min_rank_table_lca_2("rowid_3");
    # CREATE INDEX IF NOT EXISTS index_min_min_rank_table_lca_2
    # ON min_rank_table_lca_2("min");



    # CREATE TEMPORARY table max_rank_table_lca_2 AS SELECT m.entryid, m.rowid, m.rowid_3, m."TimeStamp", m.labels_predictions, t.max FROM (
    # SELECT entryid, MAX(rank_2) AS max
    # FROM rank_table_lca_2
    # GROUP BY entryid ) t JOIN rank_table_lca_2 m ON m.entryid = t.entryid AND t.max = m.rank_2;

    # CREATE INDEX IF NOT EXISTS index_entryid_max_rank_table_lca_2 ON max_rank_table_lca_2(entryid);

    # CREATE INDEX IF NOT EXISTS index_labels_predictions_max_rank_table_lca_2
    # ON max_rank_table_lca_2(labels_predictions); CREATE INDEX IF NOT EXISTS index_timestamp_max_rank_table_lca_2
    # ON max_rank_table_lca_2("TimeStamp");


    # CREATE INDEX IF NOT EXISTS index_rowid_max_rank_table_lca_2 ON max_rank_table_lca_2("rowid");
    # CREATE INDEX IF NOT EXISTS index_rowid3_max_rank_table_lca_2 ON max_rank_table_lca_2("rowid_3");
    # CREATE INDEX IF NOT EXISTS index_max_max_rank_table_lca_2
    # ON max_rank_table_lca_2("max");


    # CREATE TEMPORARY TABLE postive_candidate_table_2_lca AS
    # SELECT generate_series(min_t.rowid_3, max_t.rowid_3) FROM
    # max_rank_table_lca_2 max_t JOIN min_rank_table_lca_2 min_t ON max_t.entryid = min_t.entryid; CREATE INDEX IF NOT EXISTS index_min_postive_candidate_table_2_lca
    # ON postive_candidate_table_2_lca("generate_series");

    # CREATE TEMPORARY TABLE negative_table_lca_2 AS SELECT * FROM negative_table_lca WHERE rowid_3 IN (SELECT * FROM postive_candidate_table_2_lca);

    # CREATE INDEX IF NOT EXISTS index_rowid3_negative_table_lca_2 ON negative_table_lca_2("rowid_3");

    # CREATE TABLE cag."test_deploy_lca" AS SELECT * FROM negative_table_lca WHERE (rowid_3 NOT IN (SELECT * FROM postive_candidate_table_2_lca)) ORDER BY "rowid", "PatientID";


    # CREATE TEMPORARY TABLE diagnostic_data AS
    # SELECT * FROM cag."test_deploy_rca" 
    # UNION
    # SELECT * FROM cag."test_deploy_lca";



    # UPDATE cag."test_table" SET diagnostic_data = TRUE WHERE rowid IN (SELECT rowid FROM diagnostic_data);