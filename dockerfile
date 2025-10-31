FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install --with-deps

COPY . .

EXPOSE 5000

CMD sh -c "gunicorn --bind 0.0.0.0:${PORT:-5000} --workers 1 --threads 4 --timeout 600 --keep-alive 5 server:app"