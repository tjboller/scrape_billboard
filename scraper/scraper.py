import argparse
import requests
from datetime import datetime, timedelta
from typing import Union
import pandas as pd

from bs4 import BeautifulSoup

from utils import logging
from utils.config import __config__
from db import db_connection as db

LOGGER = logging.get_logger(__name__)


def _get_session():
    session = requests.Session()
    session.mount(
        __config__['base_url'],
        requests.adapters.HTTPAdapter(max_retries=__config__['max_retries']),
    )
    return session


SESSION = _get_session()


def download_billboard(
        max_date: str = None,
        min_date: str = None,
        if_exists: str = 'replace'
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
    query_date = max_date - timedelta(days=7)

    data = []
    for chart in __config__['chart_names']:

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
    return db.DbConnection().df_dump(
        all_charts,
        __config__['table_name'],
        if_exists=if_exists
    )


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

    titles = [
        title.text.strip() for title in
        soup.select(f'[class*="{title_tag}"]')]
    artists = [
        title.text.strip() for title in
        soup.select(f'[class*="{artist_tag}"]')]

    assert titles or artists, "Data could not be parsed"
    if not artists:
        titles, artists = artists, titles
        titles = ''

    chart_length = max(len(titles), len(artists))
    chart_data = {
        'artist': artists,
        'title': titles,
        'rank': list(range(1, chart_length + 1)),
        'chart': chart,
        'week': _parse_date(soup, date_tag)}
    return pd.DataFrame(chart_data)


def _parse_date(soup: BeautifulSoup, date_tag: str) -> str:
    date_element = soup.select_one(f'[class*="{date_tag}"]').text.strip()
    date_element = date_element.replace('Week of ', '')
    return datetime.strptime(
        date_element, "%B %d, %Y"
    ).strftime('%Y-%m-%d')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-maxd', '--max_date', help="max date to search")
    parser.add_argument('-mind', '--min_date', help="min date to search")
    parser.add_argument('-if_exists', '--if_exists', help="Whether to 'replace', or 'append")

    args = parser.parse_args()

    download_billboard(
        max_date=args.max_date,
        min_date=args.min_date,
        if_exists=args.if_exists
    )
