FROM ubuntu:18.04
MAINTAINER Artem
RUN apt-get update -y && apt-get install -y python3-pip python-dev build-essential
ADD . /flask-app
WORKDIR /flask-app
RUN pip3 install -r requirements.txt
ENTRYPOINT ["python3"]
CMD ["flask_app.py"]