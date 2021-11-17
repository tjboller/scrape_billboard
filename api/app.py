from flask import Flask, request
from flask_restx import Api, Resource, reqparse

from api import service

flask_app = Flask(__name__)
app = Api(
    app=flask_app,
    title='Billboard Data',
    version='0.1',
    description='Get data from billboard.com'
)
namespace = app.namespace(
    'billboard',
    description='Billboard Chart Data'
)


@namespace.route("/chartInfo")
class chartInfo(Resource):
    def get(self):
        return service.get_chart_info()


query_charts_parser = reqparse.RequestParser()
query_charts_parser.add_argument("artist", type=str)
query_charts_parser.add_argument("title", type=str)
query_charts_parser.add_argument("max_date", type=str)
query_charts_parser.add_argument("min_date", type=str)
query_charts_parser.add_argument("max_rank", type=str)
query_charts_parser.add_argument("min_rank", type=str)
query_charts_parser.add_argument("chart", type=str)
query_charts_parser.add_argument("max_results", type=int, default=1000)


@namespace.route(
    "/chart",
)
@namespace.doc(params={
    'artist': 'Name of the artist of interest (case sensitive)',
    'title': 'Name of the title of interest (case sensitive)',
    'chart': 'The name of the chart to query',
    'max_results': 'Number of rows returned',
    'max_date': 'Max date (YYYY-MM-DD)',
    'min_date': 'Min date (YYYY-MM-DD)',
    'max_rank': 'Max rank of interest',
    'min_rank': 'Min rank of interest'
})
class queryCharts(Resource):

    @namespace.expect(query_charts_parser)
    def get(self):
        return service.query_chart(
            max_results=request.args.get('max_results'),
            artist=request.args.get('artist'),
            title=request.args.get('title'),
            max_date=request.args.get('max_date'),
            min_date=request.args.get('min_date'),
            max_rank=request.args.get('max_rank'),
            min_rank=request.args.get('min_rank'),
            chart=request.args.get('chart'),
        )


top_queries = reqparse.RequestParser()
top_queries.add_argument("max_date", type=str)
top_queries.add_argument("min_date", type=str)
top_queries.add_argument("top_n", type=int, default=50)

@namespace.route(
    "/topArtists/<chart>",
)
@namespace.doc(params={
    'chart': 'The name of the chart to query',
    'top_n': 'Return the top N artists',
    'max_date': 'Max date (YYYY-MM-DD)',
    'min_date': 'Min date (YYYY-MM-DD)',
})
class topArtists(Resource):

    @namespace.expect(top_queries)
    def get(self, chart):
        return service.top_artists(
            chart=chart,
            max_date=request.args.get('max_date'),
            min_date=request.args.get('min_date'),
            top_n=request.args.get('top_n')
        )


@namespace.route(
    "/topTitles/<chart>",
)
@namespace.doc(params={
    'chart': 'The name of the chart to query',
    'top_n': 'Return the top N titles',
    'max_date': 'Max date (YYYY-MM-DD)',
    'min_date': 'Min date (YYYY-MM-DD)',
})
class topTitles(Resource):

    @namespace.expect(top_queries)
    def get(self, chart):
        return service.top_titles(
            chart=chart,
            max_date=request.args.get('max_date'),
            min_date=request.args.get('min_date'),
            top_n=request.args.get('top_n')
        )
