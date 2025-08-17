FROM mcr.microsoft.com/playwright/python:v1.49.1-jammy

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Camoufox needs to fetch its fingerprints
RUN python3 -m camoufox fetch

# Add app files
COPY . .

# Launch script
CMD ["python3", "main.py"]