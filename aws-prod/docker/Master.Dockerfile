FROM python:3.9-slim

WORKDIR /app

# Copy and install dependencies
COPY docker/requirements.txt .
RUN pip install -r requirements.txt

# Copy application files
COPY master .
COPY master/kaggle.json /root/.config/kaggle/
RUN chmod 600 /root/.config/kaggle/kaggle.json
RUN pip install watchdog

ENV FLASK_ENV=development
ENV FLASK_DEBUG=1
ENV FLASK_APP=app/master.py
ENV PYTHONUNBUFFERED=1
# Expose Flask port
EXPOSE 5000

# Run the master node server
#CMD ["flask", "run", "--host=0.0.0.0", "--port=5001", "--reload"]
#CMD ["python", "master.py"]
CMD ["python", "-m", "flask", "run", "--host=0.0.0.0", "--port=5001", "--reload"]
