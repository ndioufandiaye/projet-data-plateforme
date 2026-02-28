FROM python:3.11

WORKDIR /app

RUN apt-get update && apt-get install -y \
    curl \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

# ... (le reste reste identique)
RUN pip install --no-cache-dir marimo pyspark==3.5.0 pandas
# ...

RUN pip install --no-cache-dir uv

# COPY pyproject.toml uv.lock* ./
# RUN uv sync --no-dev

COPY requirements.txt .
RUN pip install -r requirements.txt

RUN pip install marimo pyspark

RUN mkdir -p /app/notebooks /app/data

EXPOSE 8080

CMD ["uv", "run", "marimo", "edit", "notebooks/delta_lake_example.py", "--host", "0.0.0.0", "--port", "8080", "--no-token"]
#CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8080", "--no-browser", "--allow-root"]