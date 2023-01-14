FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN python -m pip install -r requirements.txt

WORKDIR /app
COPY /bna /app/bna
COPY /main.py /known_addresses.json /app/
RUN mkdir /app/state

RUN adduser -u 5678 --disabled-password --gecos "" bnauser && chown -R bnauser /app
USER bnauser

CMD ["python", "main.py"]
