# syntax=docker/dockerfile:1
   
FROM amancevice/pandas:2.0.0-alpine
WORKDIR /app
COPY . .
RUN apk update 
RUN apk add alpine-sdk python3-dev
RUN pip install scrapy openpyxl sqlalchemy PySQLite fastapi uvicorn[standard] pipenv
CMD ["uvicorn --workers=4 main:app --root-path /api --port 12001", "./main.py"]

EXPOSE 3000
