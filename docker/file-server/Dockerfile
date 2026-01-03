FROM nginx:alpine

# Copy nginx config
COPY nginx.conf /etc/nginx/nginx.conf

# Create directory for data
RUN mkdir -p /usr/share/nginx/html/data

EXPOSE 8888
