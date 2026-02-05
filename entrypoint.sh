#!/bin/sh

set -e

# echo "Faking migration for token_blacklist..."
# python manage.py migrate token_blacklist --fake

echo "Applying all remaining migrations..."
python manage.py makemigrations
python manage.py migrate

echo "Starting Gunicorn server..."
exec gunicorn Almog1.wsgi:application --bind 0.0.0.0:8000 --workers 3
