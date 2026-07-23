FROM alphapulse-crawler

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        fluxbox \
        nginx-light \
        novnc \
        websockify \
        x11vnc \
        xvfb \
    && rm -rf /var/lib/apt/lists/*

COPY scripts/start_guba_browser.sh /usr/local/bin/start-guba-browser
COPY docker/guba-browser/nginx-cdp.conf /etc/nginx/nginx-cdp.conf

RUN chmod +x /usr/local/bin/start-guba-browser

CMD ["/usr/local/bin/start-guba-browser"]
