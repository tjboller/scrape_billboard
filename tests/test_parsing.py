import os
import pytest
import pandas as pd

from scraper import scraper


CURR_DIR = os.path.dirname(os.path.abspath(__file__))
CHARTS = [
    'hot-100',
    'billboard-200',
    'artist-100'
]


def read_test_data(file):
    data_path = os.path.join(CURR_DIR, 'test_data', f'{file}.txt')
    with open(data_path, 'r') as f:
        return f.read()


TEST_DATA = [read_test_data(file) for file in CHARTS]


@pytest.mark.parametrize('html,chart', zip(TEST_DATA, CHARTS), ids=CHARTS)
def test_parsing(html, chart):
    chart_df = scraper._parse_soup(html, chart)
    assert len(chart_df) > 0
    assert isinstance(chart_df, pd.DataFrame)
    assert set(chart_df.columns) == {'artist', 'title', 'chart', 'week', 'rank'}
    assert chart_df.chart.values[0] == chart
    assert chart_df.week.nunique() == 1
    if chart == 'artist-100':
        assert chart_df.title.unique() == ['']
