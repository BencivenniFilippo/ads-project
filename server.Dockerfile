FROM python:3.9-slim
RUN apt-get update
#RUN apt-get install -y "any package you like"

WORKDIR /app

COPY requirements.txt ./
RUN pip3 install -r requirements.txt

COPY ./Texts/ ./Texts
COPY server.py ./

CMD [ "python", "server.py" ]