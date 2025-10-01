# Используем чистый, легкий образ Python
FROM python:3.11-slim

WORKDIR /app

# Копируем файл с зависимостями и устанавливаем их
COPY scripts-requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Копируем ваш исходный код и .env файл, чтобы они были доступны внутри контейнера
COPY ./src ./src
COPY .env .
