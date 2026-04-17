#!/bin/bash
# Setup nginx + Let's Encrypt SSL for genai.codeen.in.net
# Run on the VM: bash setup_ssl.sh

set -e

DOMAIN="genai.codeen.in.net"
EMAIL="asb.bharath601@gmail.com"

echo "=== 1/4 Installing nginx + certbot ==="
sudo apt update
sudo apt install -y nginx certbot python3-certbot-nginx

echo "=== 2/4 Creating nginx config ==="
sudo tee /etc/nginx/sites-available/$DOMAIN > /dev/null << 'EOF'
server {
    listen 80;
    server_name genai.codeen.in.net;

    # Let's Encrypt challenge
    location /.well-known/acme-challenge/ {
        root /var/www/html;
    }

    # Redirect all HTTP to HTTPS (certbot will add this after cert is issued)
    location / {
        return 301 https://$host$request_uri;
    }
}
EOF

# Enable the site
sudo ln -sf /etc/nginx/sites-available/$DOMAIN /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t && sudo systemctl reload nginx

echo "=== 3/4 Getting SSL certificate ==="
sudo certbot --nginx -d $DOMAIN --non-interactive --agree-tos -m $EMAIL

echo "=== 4/4 Adding reverse proxy config ==="
sudo tee /etc/nginx/sites-available/$DOMAIN > /dev/null << 'EOF'
server {
    listen 80;
    server_name genai.codeen.in.net;
    return 301 https://$host$request_uri;
}

server {
    listen 443 ssl;
    server_name genai.codeen.in.net;

    ssl_certificate /etc/letsencrypt/live/genai.codeen.in.net/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/genai.codeen.in.net/privkey.pem;
    include /etc/letsencrypt/options-ssl-nginx.conf;
    ssl_dhparam /etc/letsencrypt/ssl-dhparams.pem;

    # Max upload size (10 GB for large CSV files)
    client_max_body_size 10G;

    # Proxy all requests to FastAPI on port 8000
    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # Timeouts for long DuckDB queries
        proxy_read_timeout 120s;
        proxy_connect_timeout 10s;
        proxy_send_timeout 120s;
    }
}
EOF

sudo nginx -t && sudo systemctl reload nginx

echo ""
echo "=== DONE ==="
echo "Backend is now available at: https://genai.codeen.in.net"
echo "SSL auto-renewal is handled by certbot systemd timer."
echo ""
echo "Next steps:"
echo "  1. Update server/.env: FRONTEND_URL=https://g-chat-xi.vercel.app"
echo "  2. Restart uvicorn"
echo "  3. Update Google OAuth redirect URI to: https://genai.codeen.in.net/api/auth/google/callback"
echo "  4. Update Vercel env: NEXT_PUBLIC_API_URL=https://genai.codeen.in.net"
