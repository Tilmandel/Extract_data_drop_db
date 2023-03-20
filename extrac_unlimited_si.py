import json
import logging
import sqlite3
import time
import warnings

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from seleniumwire import webdriver as wire_wd

logger = logging.getLogger('seleniumwire')
logger.setLevel(logging.ERROR)
warnings.filterwarnings("ignore")
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


def extract_cookies():
    print("EXCTRACTING COOKIS IN HEADLESS MODE")
    wire_option = wire_wd.ChromeOptions()
    wire_option.add_argument('--headless')
    wire_option.add_argument("--no-sandbox")
    wire_option.add_argument("--disable-dev-shm-usage")
    wire_option.add_argument('disable-infobars')

    wire_option.add_experimental_option("excludeSwitches", ["enable-logging"])
    wire_option.add_argument('--disable-logging')
    wire_option.add_argument("--log-level=OFF")
    chrome_wire = wire_wd.Chrome("chromedriver.exe", options=wire_option, service_log_path="null")
    wire_report_wait = WebDriverWait(chrome_wire, 60)
    check_login_1 = login_function(wire_report_wait, chrome_wire)

    chrome_wire.get("https://internal.domain.net/search.do?uri=%2Fhome.do%3F")
    all_stuff = {k: v for k, v in enumerate(chrome_wire.requests) if k <= 40}
    print(all_stuff[39].response.headers['Set-Cookie'])
    print("EXCTRACTING DONE")
    print("[RETURNING COOKIS TO BE USED BY request.get()]")
    chrome_wire.close()
    return all_stuff[39].response.headers['Set-Cookie']


def login_function(report_wait, report_driver_chromium):
    report_driver_chromium.get("http://internal.domai.net/")
    report_wait.until(EC.presence_of_element_located((By.ID, "inputpassword")))
    print("PASSWORD FORM FOUND")
    login = report_driver_chromium.find_element(By.ID, "inputusername")
    password = report_driver_chromium.find_element(By.ID, "inputpassword")
    login_button = report_driver_chromium.find_element(By.ID, "login")
    login.send_keys("login")
    password.send_keys("password")
    login_button.click()
    print("LOGIN WENT OK")
    return True


def post_to_gsnow(readed_to_string, cookie=""):
    count = 0
    with open("post_headers.json") as file:
        data_for_post = json.load(file)

    data_for_post['headers']["Cookie"] = str(data_for_post['headers']["Cookie"].format(cookie))
    data_for_post['payload']["sysparm_full_query"] = str(
        data_for_post['payload']["sysparm_full_query"].format(readed_to_string))
    data_for_post['payload']["sysparm_query"] = str(
        data_for_post['payload']["sysparm_query"].format(readed_to_string))
    data_for_post['payload_for_next_page']["sysparm_query"] = str(
        data_for_post['payload_for_next_page']["sysparm_query"].format(readed_to_string))
    r = requests.post("https://internal.domain.net/for_report.do", data=data_for_post['payload'], allow_redirects=True,
                      headers=data_for_post['headers'], verify=False)
    soup = BeautifulSoup(r.content, 'html.parser')
    print(r.status_code)
    tbody = soup.find_all("tbody", {"class": "list2_body"})
    tbody = tbody[0].find_all("tr", {"record_class": "cmdb_rel_group"})
    print(len(tbody))
    headers = soup.find_all('th', {"data-type": "list2_hdrcell"})
    all_columns_name = [item['name'] for item in headers if 'name' in str(item)]
    if not temp2:
        temp2.append(all_columns_name)
        print(all_columns_name)
    for tr in tbody:
        all_td = tr.find_all('td', {"class": "vt"})
        all_data_needed_first = dict(zip(all_columns_name, all_td[:-1]))
        temp = []
        for item in all_data_needed_first:
            if all_data_needed_first[item].text.endswith("..."):
                if item in ["group.u_contact_notes", "group.u_group_chat"]:
                    temp.append(str(all_data_needed_first[item].get("title")).strip())
                else:
                    temp.append(str(all_data_needed_first[item].get("title")).strip())
                continue
            else:
                temp.append(str(all_data_needed_first[item].text).strip())
        count += 1
        temp2.append(temp)
    return count


def extraction_from_gsnow(until_end, count, number_records):
    with open(r"path_for_asset_tag_list_tomuch.txt") as file_si:
        readed = file_si.read()

        readed_to_string = readed.strip().replace("\n", ",")
        full_list = readed_to_string.split(",").copy()
        full_list = [si for si in full_list if si.startswith("AT")]
        full_list = full_list[:number_records]
    for x in range(0, len(full_list), 100):
        if x == 0:
            until_end += len(full_list)
        readed_to_string = ",".join(full_list[x:x + 100])
        count_output = post_to_gsnow(cookie=cookie, readed_to_string=readed_to_string)
        until_end -= len(full_list[x:x + 100])
        count += count_output
        print(f'[UNTIL END]:{until_end}')
        print(f"[ALREADY EXTRACTED]: {count}")
    df = pd.DataFrame(temp2[1:])
    df.drop_duplicates(inplace=True)

    df.rename(columns=dict(zip(df.columns, temp2.pop(0))), inplace=True)
    print(f"[WHOLE EXTRACTION TOOK]:{round(time.time() - start, 2)}")
    new_columns = ['CI', 'Class', 'Group', 'Type', 'Contact Notes', 'Group Email DL', 'Group Chat', 'Manager_GPN',
                   'Manager', 'Deputy_GPN', 'Deputy', 'Group Phone', 'Description', 'Service_Schedule']
    df.rename(columns=dict(zip(df.columns, new_columns)), inplace=True)
    return df


def update_main_table(conn, data):
    if not data.empty:
        print("[UPDATING EXISTING RECORD]")
        position = conn.cursor()
        cursor_p = position.execute('SELECT * FROM support_unit_db')
        names = []
        for description in cursor_p.description:
            names.append(description[0])
        string_for_updating = "".join(f"[{column}] = ?," for column in names[1:])
        string_for_updating = "Update support_unit_db SET " + string_for_updating.strip()[:-1] + " WHERE [index] = ?"
        string_for_updating_base_cus_columns = "Update base_for_customizable_columns SET " + "[CI]=?,[Type]=?,[Group]=?" + " WHERE [index] = ?"
        print(string_for_updating)
        for x in range(len(data)):
            all_values = list(data.iloc[x].values)
            base_cuz_cols_values = [all_values[1], all_values[4],all_values[3]]
            index = str(all_values.pop(0))
            all_values.append(index)
            base_cuz_cols_values.append(index)
            print(all_values)
            position.execute(string_for_updating, all_values)
            position.execute(string_for_updating_base_cus_columns, base_cuz_cols_values)
            conn.commit()
        conn.commit()
        return


def delete_not_existing_from_db(conn, index_to_delete):
    if index_to_delete and None not in index_to_delete:
        position = conn.cursor()
        for index in index_to_delete:
            position.execute(f'DELETE FROM support_unit_db WHERE [index] = {index}')
            position.execute(f'DELETE FROM base_for_customizable_columns WHERE [index] = {index}')
            conn.commit()
        return True
    else:
        return False


def add_not_existing_in_db(data):
    position = conn.cursor()
    cursor_p = position.execute('select * from support_unit_db')
    names = []
    for description in cursor_p.description:
        names.append(description[0])
    string_for_updating = "".join(f"[{column}]," for column in names[:])
    values = "?," * len(names)
    string_for_updating_base_cus_columns = "INSERT INTO base_for_customizable_columns (" + "[index],[CI],[Type],[Group],[Flag],[Custom_Commentary]" + f") VALUES (?,?,?,?,?,?);"
    string_for_updating = "INSERT INTO support_unit_db (" + string_for_updating.strip()[
                                                            :-1] + f") VALUES ({values[:-1]});"
    for x in range(len(data)):
        all_values = list(data.iloc[x].values)
        all_values.insert(0, str(int(data.iloc[x].name)))
        values_for_commentary = [all_values[0],all_values[1],all_values[4],all_values[3],"No Data","No Data" ]
        position.execute(string_for_updating, all_values)
        position.execute(string_for_updating_base_cus_columns, values_for_commentary)
        conn.commit()
    conn.commit()
    return


def clean_duplicates(conn, table="support_unit_db"):
    all_si_from_db = pd.read_sql_query(f"SELECT CI from {table}", conn)
    for si in all_si_from_db['CI'].unique().tolist():
        query = f"SELECT * FROM {table} WHERE CI LIKE '%{si.split(' ')[-1].strip()}'"
        data_from_db = pd.read_sql_query(query, conn)
        index_from_db = data_from_db["index"].tolist()
        columns = data_from_db.columns.tolist()
        data_from_db = pd.DataFrame(data_from_db.values.tolist(), columns=columns, index=index_from_db)
        unique_records = data_from_db[data_from_db.columns[1:5]].drop_duplicates(keep="first").index.to_list()
        data_from_db.apply(lambda x: delete_not_existing_from_db(conn=conn, index_to_delete=[
            x.name]) if x.name not in unique_records else "False",
                           axis=1)


if __name__ == "__main__":
    path_to_db = fr'path_for_dB.db'
    pd.options.display.max_columns = 30
    pd.options.display.max_info_columns = 5000
    pd.options.display.max_info_rows = 5000
    pd.options.display.width = 5000
    pd.options.display.max_rows = 5000
    start = time.time()
    temp2 = []
    count = 0
    cookie = extract_cookies()
    until_end = 0
    raw_new_data_extracted = extraction_from_gsnow(until_end, count, -1)
    conn = sqlite3.connect(path_to_db)
    position = conn.cursor()
    last_index = int(position.execute("SELECT MAX([index]) FROM support_unit_db").fetchone()[0]) + 1
    position.close()
    conn.close()
    for item in raw_new_data_extracted["CI"].unique():
        conn = sqlite3.connect(path_to_db)
        position = conn.cursor()
        query = f"SELECT * FROM support_unit_db WHERE CI LIKE '%{item.split(' ')[-1].strip()}'"
        data_from_db = pd.read_sql_query(query, conn)
        index_from_db = data_from_db["index"].tolist()
        columns = data_from_db.columns.tolist()
        data_from_db = pd.DataFrame(data_from_db.values.tolist(), columns=columns, index=index_from_db)
        last_index = int(position.execute("SELECT MAX([index]) FROM support_unit_db").fetchone()[0]) + 1
        unique_records = data_from_db[data_from_db.columns[1:]].drop_duplicates(keep="first").index.to_list()
        data_from_db.apply(lambda x: delete_not_existing_from_db(conn=conn, index_to_delete=[
            x.name]) if x.name not in unique_records else "False",
                           axis=1)
        data_from_db.apply(lambda x: data_from_db.drop(axis="index", index=x.name,
                                                       inplace=True) if x.name not in unique_records else "False",
                           axis=1)
        new_data_extracted = raw_new_data_extracted.sort_values(by="Group", ascending=False)
        groups_to_add = new_data_extracted[new_data_extracted["CI"].str.endswith(item.split(' ')[-1])].query(
            f"Group not in {data_from_db['Group'].unique().tolist()}")
        to_update = new_data_extracted[new_data_extracted["CI"].str.endswith(item.split(' ')[-1])]
        to_update = to_update[~to_update["Type"].str.contains("End User")]

        if not to_update.empty:
            data_from_db = data_from_db.drop_duplicates(keep="first")
            to_update = to_update.drop_duplicates(keep="first")
            data_from_db.sort_values(["Group", "Type"], ignore_index=True, inplace=True, ascending=True)
            to_update.sort_values(["Group", "Type"], ignore_index=True, inplace=True, ascending=True)
            to_update['index'] = data_from_db['index'].astype(str)
            check_for_updating = data_from_db[data_from_db.columns[1:]].values.tolist() == to_update[
                to_update.columns[:-1]].values.tolist()
            if not check_for_updating:
                print(f"check_for_updating True if not needed: {check_for_updating}")
                print(f"[WORKING WITH ITEM]:{item}")
                print(f"[LAST INDEX IN DB:{last_index}")
                print(data_from_db[data_from_db.columns[1:]].values == to_update[
                   to_update.columns[:-1]].values)
                print(data_from_db)
                print(to_update)
                conn = sqlite3.connect(path_to_db)
                position = conn.cursor()
                to_update.reset_index(drop='index', inplace=True)
                data_from_db.reset_index(inplace=True)
                data_from_db.drop(columns=['level_0'], inplace=True)
                data_from_db.update(to_update[to_update.columns[:-1]])

                update_main_table(conn, data_from_db)
                conn.commit()
                if (len(to_update) > len(data_from_db)) and not to_update.empty:
                    to_add_or_delete = pd.concat(
                        [data_from_db[data_from_db.columns[1:]], to_update[to_update.columns[:-1]]]).drop_duplicates(
                        keep=False)
                    to_add_or_delete.index = np.arange(start=last_index, stop=last_index + len(to_add_or_delete))
                    add_not_existing_in_db(to_add_or_delete)
                    conn.commit()
                if len(to_update) < len(data_from_db):
                    to_add_or_delete = pd.concat(
                        [data_from_db[data_from_db.columns[1:]], to_update[to_update.columns[:-1]]]).drop_duplicates(
                        keep=False)
                    index_to_remove = to_add_or_delete.index.to_list()
                    index_to_remove_from_db = [data_from_db.iloc[index]['index'] for index in
                                               to_add_or_delete.index.to_list()]
                    if None not in index_to_remove_from_db:
                        delete_not_existing_from_db(conn, index_to_remove_from_db)
                    conn.commit()
                print("=" * 50)
                conn.commit()
                position.close()
                conn.close()
        else:
            if data_from_db.empty:
                print("THIS WILL BE ADDED")
                groups_to_add.sort_values(["Group", "Type"], ignore_index=True, inplace=True, ascending=False)
                groups_to_add = groups_to_add.drop_duplicates(keep="first")
                groups_to_add.index = np.arange(start=last_index, stop=last_index + len(groups_to_add))
                add_not_existing_in_db(groups_to_add)
                conn.commit()
                print("=" * 50)

    conn = sqlite3.connect(path_to_db)
    position = conn.cursor()
    clean_duplicates(conn)
    last_index = int(position.execute("SELECT MAX([index]) FROM support_unit_db").fetchone()[0]) + 1
    quick = pd.read_sql_query("SELECT [CI],[Group] FROM support_unit_db", conn)
    unique_ci = quick['CI'].nunique()
    unique_group = quick['Group'].nunique()
    print("[Unique Service Impacted]", unique_ci)
    print("[Unique Support Groups]", unique_group)
    position.close()
    conn.close()
