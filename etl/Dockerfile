FROM python:2.7

# Add and install Python modules
ADD requirements.txt /src/requirements.txt
RUN cd /src; pip install -r requirements.txt

ADD ./ /src

# Run
CMD ["python", "/src/api.py"]
