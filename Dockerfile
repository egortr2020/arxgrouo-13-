FROM python:3.11-slim

WORKDIR /usr/root/my_app

COPY app/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./

ENV APP_MODE=server

CMD ["python", "main.py"]
