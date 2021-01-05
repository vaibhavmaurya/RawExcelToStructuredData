FROM python:3

WORKDIR /parser
VOLUME ./test /test

COPY ./requirement.txt ./
COPY ./ExcelParser ./
COPY ./test ./
COPY ./TestCode.py ./

RUN cd env
RUN python -m venv env
RUN source env/bin/activate

RUN pip install --no-cache-dir -r requirements.txt

# CMD [ "python", "./TestCode.py" ]