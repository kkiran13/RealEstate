from flask import Flask
from simple_etl import SimpleETL

app = Flask(__name__)


@app.route("/")
def home():
    return "Welcome to ETL process tool!!"


@app.route("/process")
def etl():
    obj = SimpleETL()
    obj.upload_file_to_s3()
    obj.poll_sqs()
    return "ETL Process complete. Check Postgres Database using command: " \
           "docker exec -it dockerfiles_targetdb_1 psql -U postgres -d postgres -c 'SELECT * FROM public.transactions'"

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')
