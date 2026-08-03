FROM python:3
WORKDIR /
COPY requirements.txt /requirements.txt
RUN apt-get update
RUN apt-get update --fix-missing -y
COPY bucket_report.py /bucket_report.py 
COPY slack_notify.py /slack_notify.py 
RUN pip install -r requirements.txt
CMD [ "python", "bucket_report.py" ]
