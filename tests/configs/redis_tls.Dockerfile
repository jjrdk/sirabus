FROM redis:latest

WORKDIR /app
COPY redis_entrypoint.sh /app/redis_entrypoint.sh
RUN chmod +x /app/redis_entrypoint.sh
EXPOSE 6379

ENTRYPOINT ["/app/redis_entrypoint.sh"]