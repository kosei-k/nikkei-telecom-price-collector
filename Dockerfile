FROM python:3.10.0-slim

RUN pip install --upgrade pip
WORKDIR /var/docker-python

COPY requirements.txt /var/docker-python
RUN pip install -r requirements.txt

ENTRYPOINT ["python", "app/get_price_list.py"]
