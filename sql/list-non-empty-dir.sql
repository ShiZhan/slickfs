select *
from DIRECTORY
where NAME in (
  select distinct UP
  from DIRECTORY
)