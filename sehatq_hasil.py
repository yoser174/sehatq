"""
SehatQ HL7 to PGSQL
auth: Yose
date: 13 Sep 2023
"""

import logging.config
import yaml
import sys
import configparser
import glob, os
import psycopg2

LF = b"\x0A"
CR = b"\x0D"
CRLF = CR + LF
VT = b"\x0B"
FS = b"\x1C"
ENCODING = "utf-8"
BUFFER_SIZE = 1024
RECORD_SEP = b"\x0D"  # \r #
FIELD_SEP = b"\x7C"  # |  #
REPEAT_SEP = b"\x5C"  # \  #
COMPONENT_SEP = b"\x5E"  # ^  #
ESCAPE_SEP = b"\x26"  # &  #
VERSION = "0.0.1"


HL7_DIR = "D:\\DEV\\rspc_mcu\\hasil\\*.hl7"
PG_HOST = "127.0.0.1"
PG_DB = "sehatq"
PG_USER = "postgre"
PG_PASS = "postgre"

# read ini file
config = configparser.ConfigParser()
config.read("sehatq_hasil.ini")
HL7_DIR = config["General"]["HL7_DIR"]
PG_HOST = config["General"]["PG_HOST"]
PG_DB = config["General"]["PG_DB"]
PG_USER = config["General"]["PG_USER"]
PG_PASS = config["General"]["PG_PASS"]

DEBUG = True
if config["General"]["DEBUG"] == "False":
    DEBUG = False


with open("sehatq_hasil.yaml", "rt") as f:
    config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)


def insert_result(test_name, test_code, unit, ref_range, result, flag, order_id):
    """insert new result into table results"""
    sql = """ 
    INSERT INTO results (test_name,test_code,unit,ref_range,result,flag,order_id)
     VALUES ('%s','%s','%s','%s','%s','%s','%s') """
    sql = sql % (
        test_name,
        test_code,
        unit,
        ref_range,
        result,
        flag,
        order_id,
    )

    conn = None
    try:
        conn = psycopg2.connect(
            database=PG_DB, user=PG_USER, password=PG_PASS, host=PG_HOST, port="5432"
        )
        cur = conn.cursor()
        sql_del = (
            "DELETE FROM results WHERE test_code = '"
            + test_code
            + "' AND order_id = '"
            + order_id
            + "'"
        )
        logging.info(sql_del)
        cur.execute(sql_del)
        conn.commit()
        cur.execute(sql)
        logging.info(sql)
        conn.commit()
        conn.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        sys.exit(1)
        return False
    finally:
        if conn is not None:
            conn.close()
    return True


def result_post():
    logging.info("Post result..[%s]" % HL7_DIR)
    for filename in glob.glob(HL7_DIR):
        logging.info(filename)
        b_sukses = True
        # with open(filename, "r", encoding="utf8") as f:
        with open(filename, "r") as f:
            f_read = f.read()
            logging.debug(f_read)
            f_read_split = f_read.split("\n")
            logging.debug(f_read_split)
            no_order = ""
            tanggal_mcu = ""
            no_mr = ""
            nama = ""
            kode_tes_item = ""
            nama_tes = ""
            abbr = ""
            hasil = ""
            nilai_rujukan = ""
            flag = ""
            for l in f_read_split:
                logging.info(l)
                l_split = l.split("|")
                logging.info(l_split)
                logging.info(l_split[0])
                if l_split[0] == "OBR":
                    logging.info("Observations line.")
                    no_order = l_split[3]
                    tanggal_mcu = l_split[9]
                if l_split[0] == "PID":
                    logging.info("PID line.")
                    no_mr = l_split[3]
                    nama = l_split[5].split("^")[0]
                if l_split[0] == "OBX":
                    logging.info("result")
                    kode_tes_item = l_split[3].split("^")[0]
                    nama_tes = l_split[3].split("^")[1]
                    # abbr = nama_tes
                    hasil = l_split[5]
                    nilai_rujukan_high = l_split[7].split("^")[1]
                    nilai_rujukan_low = l_split[7].split("^")[0]
                    if nilai_rujukan_high != "" and nilai_rujukan_low != "":
                        nilai_rujukan = nilai_rujukan_low + " - " + nilai_rujukan_high
                    elif nilai_rujukan_high != "" and nilai_rujukan_low == "":
                        nilai_rujukan = "<= " + nilai_rujukan_high
                    elif nilai_rujukan_high == "" and nilai_rujukan_low != "":
                        nilai_rujukan = ">= " + nilai_rujukan_low
                    else:
                        satuan = ""
                    try:
                        satuan = l_split[6].split("^")[1]
                    except Exception as e:
                        logging.warning(str(e))

                    logging.info(
                        "kode_tes_item[%s] nama_tes[%s] abbr[%s] hasil[%s] satuan[%s] nilai_rujukan[%s]"
                        % (kode_tes_item, nama_tes, abbr, hasil, satuan, nilai_rujukan)
                    )
                    if not insert_result(
                        nama_tes,
                        kode_tes_item,
                        satuan,
                        nilai_rujukan,
                        hasil,
                        flag,
                        no_order,
                    ):
                        b_sukes = False

                    # sys.exit(0)

        logging.info(
            "no_order[%s] tanggal_mcu[%s] no_mr[%s] nama[%s]"
            % (no_order, tanggal_mcu, no_mr, nama)
        )
        if not DEBUG:
            if b_sukses:
                os.remove(filename)


if __name__ == "__main__":
    print("start [%s]" % VERSION)
    result_post()
