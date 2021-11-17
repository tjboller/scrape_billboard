import requests
from datetime import datetime, timedelta
from typing import List, Dict, Union
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


# TODO: Make sure session doesn't go stale
SESSION = _get_session()


def download_billboard() -> None:
    """
        main func to download all billboard charts
        :return:
            dataframe with columns chart_name, week, title, artist, rank, score
    """
    current_date = datetime.now()
    # date has to be in past or it just times out with no error
    query_date = current_date - timedelta(days=7)

    returned_date = current_date  # dummy value to just start off the where loop
    previous_returned_date = query_date  # dummy value to just start off the where loop

    data = []
    for chart in __config__['chart_names']:

        # billboard just keeps letting you query older and older dates,
        # but just keeps giving you the oldest chart it has
        while previous_returned_date != returned_date:
            chart_data = get_chart_on_date(chart, query_date)
            previous_returned_date = returned_date
            returned_date = chart_data[0]['week']

            LOGGER.info(f"queried {returned_date} {chart}")
            data += chart_data
            query_date = query_date - timedelta(days=7)

        LOGGER.info(f"finished querying {chart}")
    df = pd.DataFrame(data)
    return db.DbConnection().df_dump(df, __config__['table_name'])


def get_chart_on_date(
        chart: str,
        date: Union[str, datetime]) -> List[Dict[str, str]]:
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
    soup = BeautifulSoup(r.text, "html.parser")

    if soup.select('table'):
        return _parse_soup(soup, chart, __config__['table_style'])
    else:
        return _parse_soup(soup, chart, __config__['list_element_style'])


def _parse_soup(soup: BeautifulSoup, chart: str, config: Dict) -> List[Dict]:
    def get_title_artist(entry):
        title = _get_attr(entry, config['title'])
        artist = _get_attr(entry, config['artist'])

        # billboard puts the artist under title if artist is the primary chart
        if artist == "":
            title, artist = artist, title
        return {'title': title, 'artist': artist}

    return [{
        'chart': chart,
        'week': _parse_date(soup, config),
        'title': get_title_artist(entry)['title'],
        'artist': get_title_artist(entry)['artist'],
        'rank': _get_attr(entry, config['rank'])
    } for entry in soup.select(config['entry'])]


def _parse_date(soup: BeautifulSoup, config: Dict) -> str:
    date_element = soup.select_one(config['date_selector'])
    return datetime.strptime(
        date_element.text.strip(), "%B %d, %Y"
    ).strftime('%Y-%m-%d')


def _get_attr(soup: BeautifulSoup, selector: str):
    element = soup.select_one(selector)
    if element:
        return element.text.strip()
    return soup.get(selector)


if __name__ == '__main__':
    download_billboard()
