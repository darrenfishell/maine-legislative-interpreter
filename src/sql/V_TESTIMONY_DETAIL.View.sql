/*
 * PRESENTS TESTIMONY DETAIL RECORDS FOR VISUALIZATION
 * AND DOWNSTREAM PROCESSING OR PARSING.
 * USES CLUSTERING OUTPUT FROM THEFUZZ AND
 * IDENTIFICATION OF HIGHEST RANKED MATCHES.
 */

--CREATE OR REPLACE VIEW V_TESTIMONY_DETAIL AS
SELECT
    th.Id,
    th.SourceDocument,
    th.RequestId,
    th.FileType,
    th.FileSize,
    th.NamePrefix,
    th.FirstName,
    th.LastName,
    th.NameSuffix,
    th.Organization,
    th.PresentedDate,
    th.PolicyArea,
    th.Topic,
    th.Created,
    th.CreatedBy,
    th.LastEdited,
    th.LastEditedBy,
    th.Private,
    th.Inactive,
    th.TestimonySubmissionId,
    th.HearingDate,
    CAST(th.LDNumber AS INTEGER) AS LD_NUMBER,
    CAST(th.legislature AS INTEGER) AS LEGISLATURE,
    nc.ANCHOR AS STANDARD_ORG
FROM TESTIMONY_HEADER th
JOIN main.V_ORG_NAME_CLUSTER nc
ON th.Organization = nc.MATCH
LIMIT 100