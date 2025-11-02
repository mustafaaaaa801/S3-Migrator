FROM python:3.11-slim

WORKDIR /app

# انسخ محتوى مجلد app مباشرة إلى /app
COPY app/ /app/

# انسخ ملف المتطلبات
COPY requirements.txt /app/

# ثبّت المكتبات
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1
EXPOSE 8000

# شغّل التطبيق
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
