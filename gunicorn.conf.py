# gunicorn.conf.py
import os
import multiprocessing

# Server socket
bind = f"0.0.0.0:{os.environ.get('PORT', '10000')}"
backlog = 2048

# Worker processes
workers = 2
worker_class = "gthread"
threads = 4
worker_connections = 1000

# Timeout settings - THIS IS THE KEY FIX
timeout = 300  # 5 minutes (vs 30 second default)
keepalive = 300
graceful_timeout = 30

# Worker lifecycle
max_requests = 1000
max_requests_jitter = 100

# Logging
loglevel = "info"
accesslog = "-"
errorlog = "-"
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'

# Performance
preload_app = True
