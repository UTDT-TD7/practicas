FROM python:3

WORKDIR /usr/src/app

COPY requirements.txt e_l.py ./
RUN pip install --no-cache-dir -r requirements.txt
RUN apt-get update && apt-get install -y vim && apt-get -y install cron
COPY etl-cron /etc/cron.d/etl-cron
# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/etl-cron
# Apply cron job
RUN crontab /etc/cron.d/etl-cron
#start daemon
# Create the log file to be able to run tail
RUN touch /var/log/cron.log
 
# Run the command on container startup
CMD cron && tail -f /var/log/cron.log


 

