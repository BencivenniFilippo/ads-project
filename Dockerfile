FROM python:3.9-slim-buster
RUN apt-get update
#RUN apt-get install -y "any package you like"
RUN pip3 install rpyc
CMD [ "/bin/bash", "-c", "while true; do bash -l; done" ]