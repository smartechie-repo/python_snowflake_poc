FROM python:3

COPY src/ /src/pysnow
WORKDIR /src/pysnow

RUN pip install -r requirements.txt

CMD [ "python", "./main.py"]
