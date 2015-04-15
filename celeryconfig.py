import sys

sys.path.append('.')

BROKER_URL = 'redis://localhost:6379/'
CELERY_RESULT_BACKEND = "redis://localhost:6379/0"

# BROKER_HOST = "localhost"
# BROKER_PORT = 5672
# BROKER_USER = "celeryuser"
# BROKER_PASSWORD = "celery"
# BROKER_VHOST = "celeryvhost"

# CELERY_RESULT_BACKEND = "amqp"
CELERY_ACCEPT_CONTENT = ['pickle', 'json', 'msgpack', 'yaml']

CELERY_IMPORTS = ("dataingestion.services.celery_tasks",)
