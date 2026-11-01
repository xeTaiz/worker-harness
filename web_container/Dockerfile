FROM nginxinc/nginx-unprivileged:1.27-alpine

COPY web_container/nginx.conf /etc/nginx/conf.d/default.conf
COPY web/ /usr/share/nginx/html/

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=5 \
    CMD wget -qO- http://127.0.0.1:8080/healthz >/dev/null 2>&1 || exit 1
