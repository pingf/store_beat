from celery import Celery

app = Celery('test')
app.config_from_object('celeryconfig')
