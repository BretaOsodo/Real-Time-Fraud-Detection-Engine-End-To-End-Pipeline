FROM apache/spark:4.0.0
USER root
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir --timeout=300 --retries=5 -r /tmp/requirements.txt
ENV PYTHONPATH="/opt/project"
USER spark
