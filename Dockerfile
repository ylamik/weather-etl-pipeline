FROM apache/airflow:2.9.1

# Copy requirements file into the container
COPY requirements.txt /requirements.txt

# Install specific Python libraries
RUN pip install --no-cache-dir -r /requirements.txt