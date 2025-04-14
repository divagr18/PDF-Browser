# pdf_rag_project/pdf_rag_project/celery.py
import os
from celery import Celery

# Set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'pdf_rag_project.settings')

# Create a Celery instance named 'pdf_rag_project'
app = Celery('pdf_rag_project')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()

# Optional: Define a debug task
@app.task(bind=True)
def debug_task(self):
    print(f'Request: {self.request!r}')