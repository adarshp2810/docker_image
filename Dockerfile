FROM python:3.9-slim

WORKDIR /app

# Copy requirements.txt and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY main.py .

# Replace the hardcoded Windows path with the container path
RUN sed -i 's|r"D:\\FAST API\\Sample_Bank_Data"|"/data"|g' main.py

# Copy the data directory into the container
COPY Sample_Bank_Data /data

# Expose the port the app runs on
EXPOSE 8000

# Command to run the FastAPI application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
