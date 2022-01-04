from db import db_connection as db
import streamlit as st
import pandas as pd
import plotly.express as px


st.set_page_config(layout='wide')

###########################
# Define Constants and functions
###########################


@st.cache
def get_data():
    chart_data = db.DbConnection().read_to_pandas('select * from billboard')
    chart_data['rank'] = chart_data['rank'].astype('int32')
    return chart_data
CHART_DATA = get_data()


@st.cache
def get_charts():
    return CHART_DATA.chart.unique()
CHARTS = get_charts()


@st.cache
def get_all_artists():
    return CHART_DATA.artist.unique()
ARTISTS = get_all_artists()


@st.cache
def get_artist_titles(chart, artist):
    return CHART_DATA[
       (CHART_DATA.chart == chart) &
       (CHART_DATA.artist.isin(artist))
    ].title.unique()


@st.cache
def get_title_history(chart, artist, title):
    histories = CHART_DATA[
       (CHART_DATA.chart == chart) &
       (CHART_DATA.artist.isin(artist)) &
       (CHART_DATA.title.isin(title))
    ]

    timerange = pd.date_range(
        start=histories.week.min(),
        end=histories.week.max(), freq='7D')
    dummy_df = pd.DataFrame({'week':  timerange.astype(str)})
    histories = pd.merge(histories, dummy_df, on='week', how='outer')
    histories['title'] = ''
    return histories.sort_values('week')




###########################
# Intro Docs
###########################

st.set_option('deprecation.showPyplotGlobalUse', False)
st.title('Billboard Chart Explorer')


###########################
# ENTITY EXPLORATION - GENES/DISEASES
###########################

st.subheader('Chart History')

chart_of_interest = st.selectbox('Chart:', CHARTS)
artists_of_interest = st.multiselect('Artist:', ARTISTS)
titles = get_artist_titles(chart_of_interest, artists_of_interest)
titles_of_interest = st.multiselect('Song:', titles)

if titles_of_interest:
    title_history = get_title_history(
        chart_of_interest, artists_of_interest, titles_of_interest)
    fig = px.line(
        title_history, x='week', y='rank', color='title', markers=True)
    (
    fig
     .update_yaxes(autorange="reversed")
     .update_layout(hovermode="x unified")
     .update_traces(connectgaps=False)
    )

    st.plotly_chart(fig, use_container_width=True)
