FROM dirkcgrunwald/flair

WORKDIR /server

COPY requirements.txt /server/
COPY worker-server.py /server/

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "worker-server.py"]