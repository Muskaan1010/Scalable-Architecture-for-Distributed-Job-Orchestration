# Use official lean Python footprint runtime layer
FROM python:3.11-slim

# Enforce immediate output flush for perfect Docker trace logging
ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Install OS compilation dependencies needed for database driver compilation wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Leverage Docker cache layer mechanics by separating package installations
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy internal application source structure mapping python paths
COPY src/ /app/src/
COPY leader_election.py /app/
COPY webhook_notifier.py /app/

# Establish internal execution bounds mapping PYTHONPATH to source root
ENV PYTHONPATH=/app/src

# Boot up into your multi-replica cluster coordinator wrapper
CMD ["python", "leader_election.py"]
