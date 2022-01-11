FROM python:3.9.0

COPY /. /.
RUN chmod +x odbcDrivers.sh
RUN ./odbcDrivers.sh


COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt


CMD ["python"]
