select *
from DIRECTORY
where NAME in (
  select NAME
  from CHECKSUM
  where MD5 in (
    select MD5
    from CHECKSUM
    group by MD5 having count(*) > 1
  )
)