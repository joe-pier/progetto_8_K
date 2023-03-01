from sqlalchemy import create_engine, text
import base64
import os
import gzip
import io

db_connection = "mysql+pymysql://6y4cp2xi5l2bibtnfhwn:pscale_pw_1hoKa7T7A5CqwDV1iL3ekfFyXBliRNe4F1H0El56H6v@eu-central.connect.psdb.cloud/project_8k?charset=utf8mb4"

engine = create_engine(db_connection, connect_args= 
    {
    "ssl": {
        "sll_ca": "/etc/ssl/cert.pem"
            }
    })

def get_data():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT file FROM GZIP_data WHERE id = 29"))
        gzipped_data = result.fetchone()[0]
    with gzip.GzipFile(fileobj=io.BytesIO(gzipped_data)) as f:
        decoded_data = f.read().decode()
    return decoded_data
     




def add_data(data):
    with engine.connect() as conn:
        file = data["file"]
        date = data["date"]
        cik = data["CIK"]
        serial = data["serial"]
        path = data["path"]
        
        # Use a parameterized query to insert binary data
        query = text("INSERT INTO `project_8k`.`GZIP_data`(`file`, `date`, `cik`, `serial`, `path`) VALUES (:file, :date, :cik, :serial, :path)")
        conn.execute(query, file=file, date=date, cik=cik, serial=serial, path=path)

def convertToBinaryData(filename):
    # Convert digital data to binary format
    with gzip.open(filename, 'r') as file:
        binaryData = file.read()
    return binaryData


def trasform_fn(filename):
     filename_ = filename.split(".")[0]
     split_file_name = list(filter(None, filename_.split("__")))
     d = {split_file_name[i]: split_file_name[i+1] for i in range(0, len(split_file_name), 2)}
     d["path"] = filename
     d["file"] = convertToBinaryData(os.path.join(input_folder,filename))
     return d

     
def main():
     dl = os.listdir(input_folder)[1:3]
     split_file_name_dict = [trasform_fn(file) for file in dl]
     # print(split_file_name_dict[0]["file"])
     [add_data(file_data) for file_data in split_file_name_dict]



if __name__ == "__main__":
     
     input_folder = "GZIP_y2016_s1000"
     test = get_data()
     print(test)