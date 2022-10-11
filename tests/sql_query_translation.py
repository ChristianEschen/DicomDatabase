from re import ASCII
import psycopg2
import pandas

CREATE TABLE test_table(
 id int8 PRIMARY KEY,
 patientid VARCHAR(255) NOT NULL,
 studyid VARCHAR(255) NOT NULL,
 entryid VARCHAR(255) NOT NULL,
 labels int8,
 "TimeStamp" TIMESTAMP
 );


INSERT INTO test_table(id, patientid, studyid, entryid, labels, "TimeStamp")
VALUES (1, 1, 1, 1, 0, '2015-01-11 00:51:14'),
        (4, 1, 1, 1, 1, '2015-01-11 00:54:14'),
        (3, 1, 1, 1, 3, '2015-01-11 00:53:14'),
        (2, 1, 1, 1, 0, '2015-01-11 00:52:14'),
        (5, 1, 1, 1, 0, '2015-01-11 00:55:14'),
        (6, 1, 2, 2, 1, '2015-01-12 08:10:14'),
        (7, 1, 2, 2, 0, '2015-01-12 08:11:14'),
        (8, 1, 2, 2, 1, '2015-01-12 08:13:25'),
        (9, 2, 1, 3, 0, '2015-01-12 09:14:25'),
        (10, 2, 1, 3, 0, '2015-01-12 09:15:25'),
        (11, 2, 1, 3, 2, '2015-01-12 09:17:25'),
        (12, 2, 1, 3, 6, '2015-01-12 09:18:25'),
        (13, 3, 1, 4, 0, '2015-01-13 07:14:25'),
        (14, 3, 1, 4, 0, '2015-01-13 07:15:25'),
        (15, 3, 1, 4, 1, '2015-01-13 07:17:25'),
        (16, 3, 1, 4, 0, '2015-01-13 07:18:25'),
        (17, 3, 1, 4, 0, '2015-01-13 07:18:25'),
        (18, 4, 1, 5, 0, '2015-01-13 03:14:25'),
        (19, 4, 1, 5, 0, '2015-01-13 03:15:25'),
        (20, 4, 1, 5, 0, '2015-01-13 03:17:25'),
        (21, 4, 1, 5, 1, '2015-01-13 03:18:25'),
        (22, 4, 2, 6, 0, '2015-01-13 03:12:13'),
        (23, 4, 2, 6, 1, '2015-01-13 03:14:07'),
        (24, 4, 2, 6, 1, '2015-01-13 03:17:05'),
        (25, 4, 2, 6, 0, '2015-01-13 03:19:42'),
        (26, 4, 2, 6, 0, '2015-01-13 03:22:51'),
        (27, 5, 1, 7, 1, '2015-02-13 03:19:42'),
        (28, 5, 1, 7, 1, '2015-02-13 03:22:51'),
        (29, 6, 1, 8, 1, '2015-02-13 03:19:42'),
        (30, 6, 1, 8, 0, '2015-02-13 03:22:51'),
        (31, 6, 1, 8, 0, '2015-02-13 03:23:42'),
        (32, 6, 1, 8, 1, '2015-02-13 03:24:51'),
        (33, 7, 1, 9, 1, '2015-02-13 03:19:42'),
        (34, 7, 1, 9, 0, '2015-02-13 03:22:51'),
        (35, 7, 1, 9, 1, '2015-02-13 03:23:42'),
        (36, 7, 1, 9, 1, '2015-02-13 03:24:51'),
        (37, 7, 1, 9, 0, '2015-02-13 03:25:51'),
        (38, 7, 1, 10, 1, '2015-02-13 03:19:42'),
        (39, 7, 1, 10, 0, '2015-02-13 03:22:51'),
        (40, 7, 1, 10, 0, '2015-02-13 03:23:42'),
        (41, 7, 1, 10, 0, '2015-02-13 03:24:51'),
        (42, 7, 1, 10, 1, '2015-02-13 03:25:51'),
        (43, 7, 1, 11, 1, '2015-02-13 03:19:42'),
        (44, 7, 1, 11, 1, '2015-02-13 03:22:51'),
        (45, 7, 1, 11, 0, '2015-02-13 03:23:42'),
        (46, 7, 1, 11, 0, '2015-02-13 03:24:51'),
        (47, 7, 1, 11, 1, '2015-02-13 03:25:51'),
        (48, 7, 1, 11, 1, '2015-02-13 03:27:51'),
        (49, 7, 1, 12, 1, '2015-02-13 03:19:42'),
        (50, 7, 1, 12, 0, '2015-02-13 03:22:51'),
        (51, 7, 1, 12, 1, '2015-02-13 03:23:42'),
        (52, 7, 1, 12, 0, '2015-02-13 03:24:51'),
        (53, 7, 1, 12, 1, '2015-02-13 03:25:51'),
        (54, 7, 1, 12, 0, '2015-02-13 03:27:51'),
        (55, 7, 1, 13, 0, '2015-02-13 03:24:51'),
        (56, 7, 1, 13, 1, '2015-02-13 03:25:51'),
        (57, 7, 1, 13, 1, '2015-02-13 03:27:51')
;
# zeros
 #(1, 2, 7, 9, 10, 13, 14, 18, 19, 20, 22, 30, 31, 34, 39, 40 ,41, 45, 46, 50, 55)
 #len= 21
# ones (4, 6, 15, 21, 23, 24, 27, 28, 29, 33, 38, 43, 44, 49, 56, 57)
# len=16


sql = """
-- 1 exclude all pr group non postives 
CREATE TEMPORARY TABLE temp_table_1 AS
SELECT * FROM "dicom_pats_v8_temp" WHERE rowid IN (
        with possibilities as (
        select distinct entryid
        from "dicom_pats_v8_temp"
        where labels in (1)
        )
        select
        t.rowid
        from
        "dicom_pats_v8_temp" t
        Left join possibilities p on t.entryid = p.entryid
        WHERE p.entryid IS NOT NULL);


-- 2 select all rows from groups not starting with the positive
CREATE TEMPORARY TABLE temp_table_2 AS
select ee.*
from temp_table_1 ee
where ee."TimeStamp" < (select min(ee2."TimeStamp")
                 from temp_table_1 ee2
                 where ee2.entryid = ee.entryid and
                       ee2.labels =1
                );
--  3 and remove from temp_table_1
CREATE TEMPORARY TABLE temp_table_3 AS
SELECT * FROM temp_table_1 WHERE rowid NOT IN 
   (SELECT rowid FROM temp_table_2);


-- 4 select all paired positives pr group from 1
CREATE TEMPORARY TABLE temp_table_4 AS
select ee.*
from temp_table_1 ee
where ee."TimeStamp" < (select min(ee2."TimeStamp")
                 from temp_table_1 ee2
                 where ee2.entryid = ee.entryid and
                       ee2.labels !=1
                );


-- 5 select all paired positives pr group from 3
CREATE TEMPORARY TABLE temp_table_5 AS
select ee.*
from temp_table_3 ee
where ee."TimeStamp" < (select min(ee2."TimeStamp")
                 from temp_table_3 ee2
                 where ee2.entryid = ee.entryid and
                       ee2.labels !=1
                );

-- 6 select groups with all positives from 1
CREATE TEMPORARY TABLE temp_table_6_0 AS
SELECT entryid FROM (
SELECT entryid, count(distinct(labels))
FROM temp_table_1
GROUP BY entryid) as dd WHERE count=1;

CREATE TEMPORARY TABLE temp_table_6 AS
SELECT * FROM temp_table_1 WHERE entryid IN (SELECT entryid FROM temp_table_6_0);

-- 7 select groups with all positives from 3
CREATE TEMPORARY TABLE temp_table_7_0 AS
SELECT entryid FROM (
SELECT entryid, count(distinct(labels))
FROM temp_table_3
GROUP BY entryid) as dd WHERE count=1;

CREATE TEMPORARY TABLE temp_table_7 AS
SELECT * FROM temp_table_3 WHERE entryid IN (SELECT entryid FROM temp_table_7_0);


-- 8 Union from 4 5 6 7
SELECT * FROM temp_table_4
UNION
SELECT * FROM temp_table_5
UNION
SELECT * FROM temp_table_6
UNION
SELECT * FROM temp_table_7;
"""

sql_0 = """
-- 1 exclude all pr group non postives 
CREATE TEMPORARY TABLE temp_table_1 AS
SELECT * FROM "dicom_pats_v8_temp" WHERE rowid IN (
        with possibilities as (
        select distinct entryid
        from "dicom_pats_v8_temp"
        where labels in (0)
        )
        select
        t.rowid
        from
        "dicom_pats_v8_temp" t
        Left join possibilities p on t.entryid = p.entryid
        WHERE p.entryid IS NOT NULL);


-- 2 select all rows from groups not starting with the positive
CREATE TEMPORARY TABLE temp_table_2 AS
select ee.*
from temp_table_1 ee
where ee."TimeStamp" < (select min(ee2."TimeStamp")
                 from temp_table_1 ee2
                 where ee2.entryid = ee.entryid and
                       ee2.labels =0
                );
--  3 and remove from temp_table_1
CREATE TEMPORARY TABLE temp_table_3 AS
SELECT * FROM temp_table_1 WHERE rowid NOT IN 
   (SELECT rowid FROM temp_table_2);


-- 4 select all paired positives pr group from 1
CREATE TEMPORARY TABLE temp_table_4 AS
select ee.*
from temp_table_1 ee
where ee."TimeStamp" < (select min(ee2."TimeStamp")
                 from temp_table_1 ee2
                 where ee2.entryid = ee.entryid and
                       ee2.labels !=0
                );


-- 5 select all paired positives pr group from 3
CREATE TEMPORARY TABLE temp_table_5 AS
select ee.*
from temp_table_3 ee
where ee."TimeStamp" < (select min(ee2."TimeStamp")
                 from temp_table_3 ee2
                 where ee2.entryid = ee.entryid and
                       ee2.labels !=0
                );

-- 6 select groups with all positives from 1
CREATE TEMPORARY TABLE temp_table_6_0 AS
SELECT entryid FROM (
SELECT entryid, count(distinct(labels))
FROM temp_table_1
GROUP BY entryid) as dd WHERE count=1;

CREATE TEMPORARY TABLE temp_table_6 AS
SELECT * FROM temp_table_1 WHERE entryid IN (SELECT entryid FROM temp_table_6_0);

-- 7 select groups with all positives from 3
CREATE TEMPORARY TABLE temp_table_7_0 AS
SELECT entryid FROM (
SELECT entryid, count(distinct(labels))
FROM temp_table_3
GROUP BY entryid) as dd WHERE count=1;

CREATE TEMPORARY TABLE temp_table_7 AS
SELECT * FROM temp_table_3 WHERE entryid IN (SELECT entryid FROM temp_table_7_0);


-- 8 Union from 4 5 6 7
SELECT * FROM temp_table_4
UNION
SELECT * FROM temp_table_5
UNION
SELECT * FROM temp_table_6
UNION
SELECT * FROM temp_table_7;
"""