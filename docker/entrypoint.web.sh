#!/bin/sh
set -eu

python manage.py migrate --noinput
python manage.py collectstatic --noinput

exec gunicorn stockalert.wsgi:application -c /app/gunicorn.conf.py
