FROM python:3.11-slim

WORKDIR /app

# copier requirements
COPY requirements.txt .

# installer dépendances
RUN pip install --no-cache-dir -r requirements.txt

# copier code
COPY . .

EXPOSE 5000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5000"]