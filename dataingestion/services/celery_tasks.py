from celery import Celery
import threading

app = Celery('tasks',backend='redis://localhost:6379/', broker='redis://localhost:6379/')
app.config_from_object('celeryconfig')

import redislite, pickle, csv
red_con = redislite.Redis()



