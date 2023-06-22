FROM python:3.8-slim-buster

WORKDIR /code

COPY . .

RUN pip install --upgrade build

RUN python3 -m build

RUN python3 -m pip install --upgrade dist/*.whl

ENTRYPOINT [ "dramax" ]

CMD [ "server" ]