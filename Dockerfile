FROM python:3.11-slim

# Install Java (required for PySpark)
RUN apt-get update && \
    apt-get install -y openjdk-21-jre-headless procps curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create an arch-agnostic symlink so JAVA_HOME works on both amd64 and arm64
RUN ln -sf /usr/lib/jvm/java-21-openjdk-$(dpkg --print-architecture) /usr/lib/jvm/java-21-openjdk

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --default-timeout=1000 -r requirements.txt

RUN mkdir -p /app/notebooks /tmp/spark && \
    chmod -R 777 /tmp/spark

EXPOSE 8080 4040

CMD ["marimo", "edit", "--host", "0.0.0.0", "--port", "8080"]