# Billboard Scraper

Scrapes data from billboard.com, puts it into a sqlite database, and surfaces 
a simple flask app. 

You can change which charts it will download in `utils/config/config.yml` but 
only "hot-100", "billboard-200", and "artist-100" have been tested.

# How to download the data

with a new venv: `pip install -r requirements.txt`
download data (will take hours): `PYTHONPATH=. python scraper/scraper.py`
run flask app: 
```
    export FLASK_APP = api/app.py
    export FLASK_ENV = development
    PYTHONPATH=. flask run
```
