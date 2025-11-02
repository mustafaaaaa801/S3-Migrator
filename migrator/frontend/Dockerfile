# ./frontend/Dockerfile
# نستخدم نسخة خفيفة من Nginx
FROM nginx:alpine

# نسخ ملفات المشروع إلى مجلد Nginx لخدمة الملفات
COPY . /usr/share/nginx/html

# فتح البورت 80
EXPOSE 80

# تشغيل Nginx في المقدمة
CMD ["nginx", "-g", "daemon off;"]
