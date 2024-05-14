--get difference with files that are different between two tabkes
CREATE TABLE "exc" AS SELECT * FROM deploy_no_monai WHERE ("DcmPathFlatten" NOT IN (SELECT "DcmPathFlatten" FROM monai_trans_deploy));
-- cine loops excluded :2047
-- Patients excludede 12


--- how many XA?

SELECT COUNT(*) FROM cag.deploy_no_monai WHERE "Modality" = 'XA';
-- ==== 12049
-- how many are transformable that are XA?
SELECT COUNT(*) FROM cag.test_deploy WHERE "Modality" = 'XA';
-- ==== 12049

-- how big is the data used?
SELECT COUNT(*) FROM cag.test_deploy;
--=== 12208


-- how many are US and OT?
SELECT COUNT(*) FROM cag.test_deploy WHERE "Modality"='OT';
--== 158
SELECT COUNT(*) FROM cag.test_deploy WHERE "Modality"='OT';
SELECT COUNT(*) FROM cag.test_deploy WHERE "Modality"='IVUS';
--==1





-- nuber of PIDS that are transformable 
SELECT COUNT(DISTINCT("PatientID")) FROM cag.monai_trans_deploy;
-- 611 patients
--CAGs 614 ( UniqueStudyInstanceUID)
-- 12207 cine loops

-- count only XA modality
SELECT COUNT(DISTINCT("PatientID")) FROM cag.monai_trans_deploy WHERE "Modality" = 'XA';
SELECT COUNT(DISTINCT("PatientID", "StudyInstanceUID")) FROM cag.monai_trans_deploy WHERE "Modality" = 'XA';

-- 611 patients
-- 612 CAGs ( UniqueStudyInstanceUID)
-- 12049 cine loops
-- One patient had another CAG the date after CAG



SELECT "PatientID", "StudyInstanceUID", COUNT(*)
FROM cag.monai_trans_deploy
GROUP BY "PatientID", "StudyInstanceUID"
HAVING COUNT(*) > 1;
