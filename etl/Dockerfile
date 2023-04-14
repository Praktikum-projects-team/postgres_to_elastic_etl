FROM python:3.10

WORKDIR /app

RUN  pip install --upgrade pip \
     && pip install -r requirements.txt

COPY . .

CMD python main.py