# Base image
FROM mcr.microsoft.com/playwright/python:v1.35.0-jammy

# 작업 디렉토리 설정
WORKDIR /app

# 필요한 종속성 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# app.py 파일 복사
COPY app.py .

# 컨테이너 실행 명령 설정
CMD ["python", "app.py"]
