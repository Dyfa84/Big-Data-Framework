{{ config(severity='warn') }}

-- A zip code must belong to the range of its state: the test fails if this query returns rows
select
    u.user_id,
    u.state,
    u.zip_code
from {{ ref('stg_users') }} u
where not exists (
    select 1
    from {{ ref('zip_ranges') }} r
    where r.state = u.state
      and cast(substr(u.zip_code, 1, 3) as integer) between r.zip_min and r.zip_max
)
