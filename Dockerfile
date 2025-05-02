FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
RUN sed -i 's|r"D:\\FAST API\\Sample_Bank_Data"|"/data"|g' main.py

COPY Sample_Bank_Data /data

EXPOSE $PORT  

CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port $PORT"]
