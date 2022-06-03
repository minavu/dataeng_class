#!/usr/bin/env python
from urllib.request import urlopen
from datetime import date

FILE_DATE = date.today().strftime("%Y-%m-%d")
FILE_NAME = f"stop_events/stopEvents_{FILE_DATE}.html"

HTML = urlopen("http://psudataeng.com:8000/getStopEvents")
with open(FILE_NAME, "wb") as file:
    file.write(HTML.read())

