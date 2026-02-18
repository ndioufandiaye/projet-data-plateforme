FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./

# Install dependencies with compatible versions
RUN pip install marimo==0.1.0 \
    && pip install uv==0.3.0


# Install system dependencies
RUN pip install --no-cache-dir -r requirements.txt


# Install uv package manager
RUN pip install --no-cache-dir uv



# Install Python dependencies using uv
# RUN uv sync --no-dev


# Create directories
## RUN mkdir -p /app/notebooks /app/data

# Expose Marimo port
EXPOSE 8080

# Start Marimo notebook server using uv run
## CMD ["uv", "run", "marimo", "edit","python", "--host", "0.0.0.0", "--port", "8080", "--no-token"]
