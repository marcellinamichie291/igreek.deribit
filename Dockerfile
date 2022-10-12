FROM amd64/python:3.7

ENV PYTHONUNBUFFERED 1

RUN pip install --upgrade pip

WORKDIR /app

COPY /app/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["-h"]
ENTRYPOINT ["/usr/local/bin/python"]
