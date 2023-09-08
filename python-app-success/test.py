from hdbcli import dbapi

import numpy as np
import pandas as pd
import datetime as dt

def parse_db_name(name):
    return '"' + name + '"'

if __name__ == "__main__":
    connection = dbapi.connect(
        address=r"84588d31-d33e-432b-868e-4825aeb1eeb8.hna0.prod-eu10.hanacloud.ondemand.com",
        port=443,
        user=r"BLACK#BLACK",
        password=r"*_?>A4SXn:N+pM?Cb$w>B/gH]+/AWN5w",
        encrypt=True, # must be set to True when connecting to HANA Cloud
        sslValidateCertificate=True, # True HC, False for HANA Express.
    )

    schema_name = "BLACK#BLACK"
    table_input_name = "DISNEY_VENDITE"
    table_output_name = "DISNEY_VENDITE_IMPORT"

    try:
        connection.setautocommit(False)
        cursor = connection.cursor()

        cursor.execute(f"SELECT * FROM {parse_db_name(schema_name) :s}.{parse_db_name(table_input_name) :s}")

        desc = cursor.description
        columns = [d[0] for d in desc]
        data = cursor.fetchall()

        data_df = pd.DataFrame(data, columns=columns)

        query = "INSERT INTO {:s}.{:s} ({:s}) VALUES ({:s})".format(
            parse_db_name(schema_name),
            parse_db_name(table_output_name),
            ", ".join(map(parse_db_name, columns)),
            ", ".join(["?"] * len(columns)),
        )

        cursor.executemany(
            query,
            (
                data_df
                .iloc[:3, :]
                .assign(
                    CLIENTE="TEST",
                    CITTA="TEST",
                    DATA=dt.date(2025, 1, 1),
                    VALUTA="TEST",
                    IMPORTO=1,
                    QUANTITA=2,
                )
                .to_records(index=False)
                .tolist()
            )
        )

        connection.commit()
    except Exception as exc:
        connection.rollback()
        e = exc
        raise exc
    finally:
        connection.close()
