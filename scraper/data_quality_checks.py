from db.db_connection import DbConnection
from utils import logging

LOGGER = logging.get_logger(__name__)


def remove_duplicates():
    DbConnection().write('''
            delete from billboard where rowid not in (
            select min(rowid)
            from billboard
            group by uuid
        )
    ''')


def check_missing_weeks():
    gaps = DbConnection().read_to_pandas(
        '''
        with chart_dates as (
            select distinct
                   week,
                   lag(week, 1) over 
                     (partition by chart order by week) as last_week,
                   chart
            from (
                select distinct week, chart from billboard
            )
        )
        select
               week,
               last_week,
               JULIANDAY(week) - JULIANDAY(last_week) as time_gap,
               chart
        from chart_dates
        where
            JULIANDAY(week) - JULIANDAY(last_week) > 13
        '''
    ).to_dict('records')
    for gap in gaps:
        LOGGER.info(f'Gap in data found: {str(gap)}')
    return gaps


def get_duplicate_uuids():
    return DbConnection().read_to_pandas(
        '''
            select
                    uuid
            from billboard
            group by uuid
            having count(uuid)>1
        '''
    ).uuid.values


def check_for_duplicates() -> bool:
    duplicate_uuids = get_duplicate_uuids()
    if len(duplicate_uuids) > 0:
        LOGGER.warning(f'{len(duplicate_uuids)} duplicate uuids found')
        return True
    else:
        return False
