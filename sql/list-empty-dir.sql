select *
from DIRECTORY
where D and not NAME in (
  select distinct UP
  from DIRECTORY
)