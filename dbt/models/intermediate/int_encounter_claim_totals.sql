-- Sum of CHARGE lines per encounter. Claims link to encounters via APPOINTMENTID.
select
    c.encounter_id,
    sum(t.amount) as claim_line_total,
    count(*)      as claim_line_count
from {{ ref('stg_claims') }} as c
inner join {{ ref('stg_claims_transactions') }} as t
    on t.claim_id = c.claim_id
where t.transaction_type = 'CHARGE'
  and c.encounter_id is not null
group by c.encounter_id
