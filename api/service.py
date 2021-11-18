from db import db_connection as db


def get_chart_info():
    return db.DbConnection().read_to_pandas(
        '''
            select 
                chart, 
                max(week) as max_week,
                min(week) as min_week,
                count(*) as num_rows
            from billboard group by chart
        ''').to_dict('records')


def query_chart(
        max_results, artist=None, title=None, max_date=None,
        min_date=None, max_rank=None, min_rank=None, chart=None,

):

    query = f'''
            select 
                *
            from billboard
            {
                _build_where_clause(
                    artist=artist, title=title, max_date=max_date, 
                    min_date=min_date, max_rank=max_rank,
                    min_rank=min_rank, chart=chart)
            }
            limit {max_results}
        '''
    return db.DbConnection().read_to_pandas(query).to_dict('records')


def top_artists(chart, max_date=None, min_date=None, top_n=None):
    return _top_query('artist', chart, max_date, min_date, top_n)


def top_titles(chart, max_date=None, min_date=None, top_n=None):
    return _top_query('title', chart, max_date, min_date, top_n)


def _top_query(query, chart, max_date=None, min_date=None, top_n=None):
    query = f'''
            select distinct
                {query},
                sum(1.0/rank) as score
            from billboard
            {
                _build_where_clause(
                    max_date=max_date, min_date=min_date, chart=chart)
            }
            group by {query}
            order by sum(1.0/rank)  desc
            limit {top_n}
        '''
    return db.DbConnection().read_to_pandas(query).to_dict('records')


def _build_where_clause(
        artist=None, title=None, max_date=None,
        min_date=None, max_rank=None, min_rank=None, chart=None):

    artist_clause = f'artist = "{artist}"' if artist else None
    title_clause = f'title = "{title}"' if title else None
    max_date = f'week <= "{max_date}"' if max_date else None
    min_date = f'week >= "{min_date}"' if min_date else None
    max_rank = f'rank <= "{max_rank}"' if max_rank else None
    min_rank = f'rank >= "{min_rank}"' if min_rank else None
    chart = f'chart = "{chart}"' if chart else None

    constraints = [
        artist_clause,
        title_clause,
        max_date,
        min_date,
        max_rank,
        min_rank,
        chart
    ]
    constraints = [constraint for constraint in constraints if constraint]
    if not constraints:
        return ''
    else:
        return f"where {' and '.join(constraints)}"
