-- An encounter cannot end before it starts.
select encounter_id, started_at, stopped_at
from {{ ref('fct_encounters') }}
where stopped_at < started_at
