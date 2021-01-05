FROM python:3

WORKDIR /parser

COPY requirement.txt ./
COPY ExcelParser ./
COPY test ./
COPY TestCode.py ./

RUN python -m venv .

RUN pip install --no-cache-dir -r requirements.txt

CMD [ "python", "./TestCode.py" ]