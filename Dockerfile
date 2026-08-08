FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ray_agent_sre/ ray_agent_sre/

EXPOSE 9100

ENTRYPOINT ["python", "-m", "ray_agent_sre.server"]
CMD ["--port", "9100", "--interval", "5"]
