import mysql.connector
import pycurl
from urllib.parse import urlencode
import json

# Your apikey and inbox_id goes in here
api_key = ''
inbox_id = ''

data = {"api_key": '',"inbox_id": ''}
send = urlencode(data)
crl = pycurl.Curl()
crl_send = pycurl.Curl()

try:
    from io import BytesIO
except ImportError:
    from StringIO import StringIO as BytesIO

buffer = BytesIO()

# This retrieves the message from the inbox
crl.setopt(crl.URL, 'https://api.textlocal.in/get_messages/')
crl.setopt(crl.POST, True)
crl.setopt(crl.POSTFIELDS, send)
crl.setopt(crl.WRITEDATA, buffer)
crl.perform()
message_byte = buffer.getvalue()
message_str = message_byte.decode()
message_dict = json.loads(message_str)
crl.close()

# Establishing connection with database
connection = mysql.connector.connect(host="", user="", passwd="", database="" )
cursor = connection.cursor(buffered=True)


false = 0
true = 0
message_list = []
number_list = []
datetime_list = []

# Format maintained to ensure sucessful operations on data
mess_format = ['8YCBH', 'FarmerID', 'VegetableName', 'Weight(kg)']


for i in message_dict["messages"]:
    try:
        message_list.append(i['message'])
        number_list.append(i['number'])
        datetime_list.append(i['date'])
    except KeyError:
        pass

# Query to check for duplicate entries
query_check = "SELECT (sent_dt) FROM sms_in WHERE sent_dt LIKE (%s)";

# Query to insert values into a database
query_insert = "INSERT INTO sms_in(sms_text, sender_number, sent_dt) VALUES (%s,%s,%s)";

# Query to display prices of commodities
query_display = "SELECT v_name, farmer_v_price FROM vegetable";


for (i, j, k) in zip(message_list, number_list, datetime_list):
    cursor.execute(query_check, (k,))
    res = cursor.rowcount
    if res == 1:
        continue
    else:
        # Inbox name 1 sends a message to the initial sender the names and prices of commodities
        if i == '8YCBH 1':
            cursor.execute(query_display)
            vegetable_display = cursor.fetchall()
            vegetable_format = vegetable_display + mess_format
            send_data = {"api_key": '',"numbers": int(j), "message" : vegetable_format}
            final_data = urlencode(send_data)
            crl_send.setopt(crl_send.URL, 'https://api.textlocal.in/send/')
            crl_send.setopt(crl_send.POST, True)
            crl_send.setopt(crl_send.POSTFIELDS, final_data)
            crl_send.setopt(crl_send.WRITEDATA, buffer)
            result = crl_send.perform()
        else:
            cursor.execute(query_insert, (i, j, k))


connection.commit()
connection.close()
