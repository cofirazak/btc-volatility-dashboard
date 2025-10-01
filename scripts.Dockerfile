# Use a clean, lightweight Python image
FROM python:3.11-slim

WORKDIR /app

# Copy the dependencies file and install them
COPY scripts-requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy your source code and .env file to make them available inside the container
COPY ./src ./src
COPY .env .
