# Snowflake masking demo: captured output

Captured on 2026-10-07 from a Snowflake trial account (Enterprise edition), after two successful
`WAREHOUSE=snowflake` runs of `patient_pipeline` on the bundled sample batch. The queries are in
[`snowflake/demo_queries.sql`](../snowflake/demo_queries.sql) and were run in Snowsight with
`use secondary roles none`. All data is synthetic (Synthea); the SSNs are in Synthea's fake 999 range.

## ANALYST

```sql
use role ANALYST;
select patient_id, first_name, last_name, ssn, birth_date, city, state, age_years
from HEALTHCARE.MARTS.DIM_PATIENTS order by patient_id limit 3;
```

| PATIENT_ID | FIRST_NAME | LAST_NAME | SSN | BIRTH_DATE | CITY | STATE | AGE_YEARS |
|---|---|---|---|---|---|---|---|
| 06f3e58e47feb3d239b225207b7e02d0271b1ce8c5579960620e135aa426ffdf | \*\*\*MASKED\*\*\* | \*\*\*MASKED\*\*\* | \*\*\*MASKED\*\*\* | 1988-01-01 | Dedham | Massachusetts | 38 |
| 087fad91eb6fe29c73dbaf60a0772b22f701bdf75d811ec6a36161961b5e76b6 | \*\*\*MASKED\*\*\* | \*\*\*MASKED\*\*\* | \*\*\*MASKED\*\*\* | 1973-01-01 | Revere | Massachusetts | 53 |
| 0b90364376a1d638b7b659554320e537615265bafb6f55c944f9d4c0a833fc5d | \*\*\*MASKED\*\*\* | \*\*\*MASKED\*\*\* | \*\*\*MASKED\*\*\* | 2015-01-01 | Boston | Massachusetts | 11 |

## PHI_READER

The same three patients, ordered by the same hash (`order by sha2(patient_id, 256)`):

| PATIENT_ID | FIRST_NAME | LAST_NAME | SSN | BIRTH_DATE | CITY | STATE | AGE_YEARS |
|---|---|---|---|---|---|---|---|
| f3aa70e2-d777-e5ba-288f-aa5469241c9d | Lavelle273 | Hilll811 | 999-15-3798 | 1988-03-26 | Dedham | Massachusetts | 38 |
| 5e30f14c-15ab-b668-fb6f-03fb3c3f4e66 | Wilburn655 | Goodwin327 | 999-89-7733 | 1973-04-12 | Revere | Massachusetts | 53 |
| 821038cb-93d8-070c-3e45-ba5c391a2601 | Hanh683 | Walsh511 | 999-72-4527 | 2015-08-16 | Boston | Massachusetts | 11 |

## Joins still work

`patient_id` is hashed the same way in every mart, so an analyst can join facts to patients:

```sql
use role ANALYST;
select count(*) as encounters_joined_to_patients
from HEALTHCARE.MARTS.FCT_ENCOUNTERS e
join HEALTHCARE.MARTS.DIM_PATIENTS p on p.patient_id = e.patient_id;
```

| ENCOUNTERS_JOINED_TO_PATIENTS |
|---|
| 1596 |

## ANALYST cannot read RAW

```sql
use role ANALYST;
select count(*) from HEALTHCARE.RAW.PATIENTS;
```

```
SQL compilation error: Schema 'HEALTHCARE.RAW' does not exist or not authorized.
Your primary role ANALYST must have USAGE or any other privilege granted on SCHEMA HEALTHCARE.RAW.
```

## Policy attachment (checked as TRANSFORMER)

- `DIM_PATIENTS`: 11 masked columns. `MASK_PII_STRING` on first/last/maiden name, SSN, driver's
  license, passport, address and ZIP; `MASK_DATE_TO_YEAR` on birth and death date;
  `MASK_PATIENT_ID` on patient_id.
- `FCT_ENCOUNTERS` and `FCT_READMISSIONS_30D`: `MASK_PATIENT_ID` on patient_id.
- Table grants on `DIM_PATIENTS`: OWNERSHIP to TRANSFORMER; SELECT to ANALYST and PHI_READER only.
- A second pipeline run succeeded with the policies attached, confirming the
  `CREATE ... IF NOT EXISTS` + `ALTER ... SET BODY` update path.
