import argparse
import requests
from datetime import datetime, timedelta
from typing import Union, List
import uuid
import pandas as pd

from bs4 import BeautifulSoup

from utils import logging
from utils.config import __config__
from db import db_connection as db
from scraper import data_quality_checks

LOGGER = logging.get_logger(__name__)


def _get_session():
    session = requests.Session()
    session.mount(
        __config__['base_url'],
        requests.adapters.HTTPAdapter(max_retries=__config__['max_retries']),
    )
    return session


SESSION = _get_session()


def update_billboard() -> None:
    most_recent_dates = db.DbConnection().read_to_pandas(
        '''
            select
                max(week) as max_week,
                chart
            from billboard
            group by chart
        '''
    ).to_dict('records')
    for chart_type in most_recent_dates:

        max_week_dt = datetime.strptime(chart_type['max_week'], '%Y-%m-%d')
        if (datetime.now() - max_week_dt).days > 6:

            # query weeks until we hit the current max week
            download_billboard(
                min_date=chart_type['max_week'],
                if_exists='append',
                charts=[chart_type['chart']]
            )
        else:
            LOGGER.info(f"{chart_type['chart']} is up to date")

    data_quality_checks.check_missing_weeks()
    data_quality_checks.check_for_duplicates()


def download_billboard(
        max_date: str = None,
        min_date: str = None,
        if_exists: str = 'append',
        charts: List[str] = None,
        remove_duplicates: bool = False
) -> None:
    """
        main func to download all billboard charts
        :param max_date:
            max_date to search, defaults to todays date
        :param min_date:
            min_date to search, defaults to full history of chart
        :param if_exists:
            whether to append or replace the data. Replaces by default
        :return:
            dataframe with columns chart_name, week, title, artist, rank, score
    """

    max_date = (
        datetime.strptime(max_date, '%Y-%m-%d')
        if max_date else datetime.now()
    )
    min_date = (
        datetime.strptime(min_date, '%Y-%m-%d')
        if min_date else datetime(1800, 1, 1)
    )

    # date has to be in past or it just times out with no error
    query_date = max_date - timedelta(days=6)

    data = []
    charts = charts if charts else __config__['chart_names']
    for chart in charts:

        returned_date = max_date  # dummy value to just start off the where loop
        previous_returned_date = query_date  # dummy value to just start off the where loop

        # billboard just keeps letting you query older and older dates,
        # but just keeps giving you the oldest chart it has
        while (previous_returned_date != returned_date) and \
                    (query_date > min_date):
            chart_data = get_chart_on_date(chart, query_date)
            previous_returned_date = returned_date
            returned_date = chart_data['week'].iloc[0]

            LOGGER.info(f"queried {returned_date} {chart}")
            data.append(chart_data)
            query_date = query_date - timedelta(days=7)

        LOGGER.info(f"finished querying {chart}")
    all_charts = pd.concat(data)
    db.DbConnection().df_dump(
        all_charts,
        __config__['table_name'],
        if_exists=if_exists
    )

    duplicates = data_quality_checks.check_for_duplicates()
    gaps = data_quality_checks.check_missing_weeks()

    if remove_duplicates and duplicates:
        LOGGER.info("Removing duplicates...")
        data_quality_checks.remove_duplicates()
        if not data_quality_checks.check_for_duplicates():
            LOGGER.info("Successfully removed duplicates")


def get_chart_on_date(
        chart: str,
        date: Union[str, datetime]) -> pd.DataFrame:
    """
    :param chart:
        The name of the chart to retrieve data from.
        Exp: hot-100, billboard-200, artist-100
    :param date:
        Date in YYYY-MM-DD form or datetime.
    :return:
        List of dictionaries containing:
            chart_name, week, title, artist, rank, score
    """

    #TODO: check if date fits YY-MM-DD if string
    if isinstance(date, datetime):
        date = date.strftime('%Y-%m-%d')

    r = SESSION.get(f'{__config__["base_url"]}/{chart}/{date}')
    r.raise_for_status()
    return _parse_soup(r.text, chart)


def _parse_soup(html: str, chart: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "html.parser")

    title_tag = __config__['title_tag']
    artist_tag = __config__['artist_tag']
    date_tag = __config__['date_tag']
    rank_tag = __config__['rank_tag']

    titles = [
        title.text.strip() for title in
        soup.select(f'[class*="{title_tag}"]')]
    artists = [
        artist.text.strip() for artist in
        soup.select(f'[class*="{artist_tag}"]')]
    ranks = [
        rank.text.strip() for rank in
        soup.select(f'[class*="{rank_tag}"]')
    ]

    week = _parse_date(soup, date_tag)
    assert titles or artists, "Data could not be parsed"
    if not artists:
        titles, artists = artists, titles
        titles = ['' for _ in artists]

    chart_data = {
        'artist': artists,
        'title': titles,
        'rank': ranks,
        'chart': chart,
        'week': week,
        'uuid': [
            _uuid(artist, title, chart, week, ranks)
            for artist, title, ranks in
            zip(artists, titles, ranks)
        ]
    }
    return pd.DataFrame(chart_data)


def _parse_date(soup: BeautifulSoup, date_tag: str) -> str:
    date_element = soup.select_one(f'[class*="{date_tag}"]').text.strip()
    date_element = date_element.replace('Week of ', '')
    return datetime.strptime(
        date_element, "%B %d, %Y"
    ).strftime('%Y-%m-%d')


def _uuid(*args: str) -> str:
    return str(uuid.uuid3(uuid.NAMESPACE_DNS, ''.join(args)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-u', '--update', help='whether you want to update the existing charts',
        dest='update', action='store_true'
    )
    parser.add_argument('-maxd', '--max_date', help="max date to search")
    parser.add_argument('-mind', '--min_date', help="min date to search")
    parser.add_argument('-if_exists', '--if_exists', help="Whether to 'replace', or 'append")
    parser.add_argument('-c', '--charts', nargs='+', help="Charts to get, defaults to charts in config")
    parser.add_argument('-d', '--remove_duplicates', help="Whether to remove duplicates if found")
    parser.set_defaults(update=False)

    args = parser.parse_args()
    if args.update:
        update_billboard()
    else:
        download_billboard(
            max_date=args.max_date,
            min_date=args.min_date,
            if_exists=args.if_exists,
            charts=args.charts,
            remove_duplicates=args.remove_duplicates
        )
