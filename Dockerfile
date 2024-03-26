#CONTAINER TEMPLATE
FROM python:3.9-slim-bookworm

RUN echo $PATH

WORKDIR /opt/cityworks_puller

#install requirements
COPY requirements.txt /opt/cityworks_puller
RUN pip install -r requirements.txt

# copy the script
COPY cityworks_puller /opt/cityworks_puller

# add the script callers to path
ENV PATH="/opt/cityworks_puller/bin:$PATH"