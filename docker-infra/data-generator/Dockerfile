FROM python:3.9-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir kafka-python faker

# Copy the data generator script into the image
COPY scripts/data_generator.py /app/data_generator.py

ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "data_generator.py"]
