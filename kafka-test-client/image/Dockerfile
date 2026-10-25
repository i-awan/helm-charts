# Build:  docker build -t <registry>/kafka-test-client:0.1.0 image/
# The script itself is NOT baked in; the chart mounts files/client.py from a ConfigMap.
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt && rm /tmp/requirements.txt
# OpenShift runs containers with a random UID in group 0
RUN mkdir -p /app && chgrp -R 0 /app && chmod -R g=u /app
WORKDIR /app
USER 1001
CMD ["python", "/app/client.py", "check"]
