-----Get the data from the MySQL--------------
select
    U.user_name,
    A.album_title
from
    users as U
    left join user_favorites as F on F.user_id = U.user_id
    left join albums as A on A.band_id = F.band_id