FROM docker.io/python:3.13-bookworm AS gcp-python-openllmetry-example
RUN apt-get update && apt-get install -y build-essential
RUN pip3 install --upgrade pip
COPY requirements.txt /etc/python-deps/requirements.txt
RUN pip3 install -r /etc/python-deps/requirements.txt
COPY src /app
WORKDIR /app
ENTRYPOINT ["/usr/bin/env", "python3", "main.py"]
