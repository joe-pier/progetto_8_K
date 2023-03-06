from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import gzip
import os
import time
import re


def get_data_by_id(id):
    """ retrieve files by id """
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT file FROM GZIP_data WHERE id = {id}"))
        gzipped_data = result.fetchone()[0]
    return gzip.decompress(gzipped_data)


def add_data(data):
    """ insert data in the database """
    add_data.counter += 1
    print(add_data.counter, round(add_data.counter/len_directory_filename_list,
          2), "% ", data["path"], round(data["size"], 2), "MB", end="\r")

    try:
        with engine.connect() as conn:
            file = data["file"]
            path = data["path"]
            year = data["year"]
            cik = data["cik"]
            # Use a parameterized query to insert binary data
            query = text(
                "INSERT INTO `project_8k`.`GZIP_data`(`file`, `path`, `cik`,`year`,`status`) VALUES (:file, :path, :cik, :year, :status)")
            conn.execute(query, file=file,  path=path,
                         status=1, year=year, cik=cik)
            return data["size"]
    except:
        print("error on")
        print(data["path"], round(data["size"], 2), "MB")
        print("\n")
        query = text(
            "INSERT INTO `project_8k`.`GZIP_data`(`path`, `cik`,`year`,`status`) VALUES (:path,:cik, :year,  :status)")
        conn.execute(query,  path=path, status=0,  year=year, cik=cik)
        return data["size"]


def convert_to_binary(filename):
    """ Convert gzip data to binary format """
    with gzip.open(filename, 'rb') as file:
        file_content = file.read()
    return gzip.compress(file_content)


def get_path_file_from_fs(filename):
    """ get the path, gzip and size of the file for the specific filename """
    d = {}
    filename_cik_serial_date = re.findall("\d+", filename)
    d["cik"] = filename_cik_serial_date[0].zfill(10)
    d["year"] = filename_cik_serial_date[-3]
    # d["serial"] = filename_cik_serial_date[-4]
    # d["date"] = filename_cik_serial_date[-3::]
    d["path"] = filename
    d["file"] = convert_to_binary(os.path.join(input_folder, filename))
    d["size"] = get_size(os.path.join(input_folder, filename))
    return d


def get_size(file_path):
    """ get size in MB for a file """
    size = os.path.getsize(file_path) / (1024 * 1024)
    return size


if __name__ == "__main__":
    """ establish connection to db """
    try:
        load_dotenv()

        host = os.getenv("HOST")
        user = os.getenv("USERNAMEDB")
        passwd = os.getenv("PASSWORD")
        db = os.getenv("DATABASE")
        db_connection = f"mysql+pymysql://{user}:{passwd}@{host}/{db}?charset=utf8mb4"

        engine = create_engine(db_connection, connect_args={
            "ssl": {
                "sll_ca": "/etc/ssl/cert.pem"
            }
        })
    except:
        print("no connection to the db")

    n = 1000
    input_folder = "GZIP_y2017_s70000"
    directory_filename_list = os.listdir(input_folder)[1:n+1]
    len_directory_filename_list = len(directory_filename_list)
    add_data.counter = 0

    start_time = time.time()
    path_file_from_fs = [get_path_file_from_fs(
        file) for file in directory_filename_list]
    total_weight = sum([add_data(file_data)
                       for file_data in path_file_from_fs])
    end_time = time.time()
    print("\r")
    print("total weight of compressed files uploaded:",
          round(total_weight, 4), "MB")
    print(
        f"total time elapsed for {len_directory_filename_list} files, of {round(total_weight,4)} MB: {round(end_time-start_time, 2)} s")

    # test = get_data_by_id(23)
    # print(test)
