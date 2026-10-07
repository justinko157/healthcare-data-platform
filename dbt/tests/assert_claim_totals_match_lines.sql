-- Synthea claims have no header total, so the encounter's claim total is defined as the sum of
-- its CHARGE lines. Recompute it straight from staging: a join fan-out anywhere between staging
-- and the mart (e.g. a duplicated provider or payer row) inflates the mart value and fails here.
with lines as (
    select c.encounter_id, sum(t.amount) as expected_total
    from {{ ref('stg_claims') }} as c
    inner join {{ ref('stg_claims_transactions') }} as t on t.claim_id = c.claim_id
    where t.transaction_type = 'CHARGE'
    group by c.encounter_id
)

select f.encounter_id, f.claim_line_total, l.expected_total
from {{ ref('fct_encounters') }} as f
inner join lines as l on l.encounter_id = f.encounter_id
where f.claim_line_total is null
   or abs(f.claim_line_total - l.expected_total) > 0.01
