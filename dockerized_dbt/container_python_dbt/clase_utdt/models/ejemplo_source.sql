with tabla as(

select *
from {{source('listen','listen_events')}}

)
select * from tabla

