#!/bin/sh
set -e

redis-server --tls-port 6379 --port 0 \
    --tls-cert-file /tls/server_certificate.pem \
    --tls-key-file /tls/server_key.pem \
    --tls-ca-cert-file /tls/ca_certificate.pem \
    --tls-auth-clients no \
    --loglevel debug

echo "Redis server started with TLS on port 6379"