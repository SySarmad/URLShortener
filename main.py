from flask import Flask, request, render_template, redirect  # import Flask to handle requests, render html and redirect
from flask_restful import Api                                # import API to set to create server
from urlparse import urlparse                                # import url_parse to insure validity of input URL
import mysql.connector as mariadb                            # import mysql.connector to connect to MySQL db
import string as s                                           # import string to create base string for decoding
import base64 as base64                                      # Base64 encoding for URL Db input



app = Flask(__name__)
api = Api(app)
host = 'ec2-52-53-236-161.us-west-1.compute.amazonaws.com'                              # URL for DB access



def check_table():
    """Creates Db table and a unique index on ID col in a MySQL database if one exists no changes will be made"""
    create_db = """CREATE DATABASE IF NOT EXISTS my_db"""
    create_table = """CREATE TABLE IF NOT EXISTS my_db.URLS(ID INT PRIMARY KEY NOT NULL AUTO_INCREMENT,URL VARCHAR(255) NOT NULL)"""
    check_index = """SHOW INDEX FROM my_db.URLS WHERE KEY_NAME = 'URLS_ID_uindex'"""
    create_index = """CREATE UNIQUE INDEX URLS_ID_uindex ON my_db.URLS (ID) """

    # DB connection and Cursor intialized
    mariadb_connection = mariadb.connect(host='mydibinstance.c3odauswfp7h.us-west-1.rds.amazonaws.com', port=3306, user='root', passwd='akmalsarm12', db='my_db')
    cursor = mariadb_connection.cursor()
    try:
        # Execute and commit Create table and Create Index SQL Statements
        cursor.execute(create_db)
        mariadb_connection.commit()
        cursor.execute(create_table)
        mariadb_connection.commit()
        cursor.execute(check_index)                       # Check if index exists otherwise create it
        i = cursor.fetchall()
        if len(i) <= 0:
            cursor.execute(create_index)
            mariadb_connection.commit()
    except mariadb.OperationalError:
        pass
    finally:
        cursor.close()                                      # Close  the cursor
        mariadb_connection.close()                          # Close the connection


def base_64_encoder(n, b=64):
    """Takes in an integer and creates a base64 string representation using letters and digits: A-a-z-Z-0-9"""
    if b <= 0 or b > 64:
        return 0
    else:
        base = s.lowercase + s.uppercase + s.digits     # base string comprised of letters and digits
        r = n % b                                       # index to be used with respect to base string
        res = base[r]                                   # result set intialized at the value of n % b
        q = n // b                                      # the count for the while loop decrementing
        while q:                                        # reduce index and create result string using q as count
            r = q % b
            q //= b
            res += base[int(r)]
        return res                                      # return encoded result string


def base_64_decoder(num, b=64):
    """Takes in an base64 encoded string and returns a decoded integer"""
    base = s.lowercase + s.uppercase + s.digits         # base string comprised of letters and digits
    res = 0                                             # result intialized at 0
    for i in xrange(len(num)):                          # loop through num and decode integer                         
        res = b * res + base.find(num[i])
    return res


@app.route('/', methods=['GET', 'POST'])
def index():
    """On GET returns template index.html, if a POST command is issued URL is shortened and returned with index.html"""
    if request.method == 'POST':
        initial_url = request.form.get('url')     # Grab url from form using POST
        if urlparse(initial_url).scheme == '':    # Check if URL is a correct url if not prepend 'http://
            initial_url = 'http://' + initial_url
        # Connect to DB, create Cursor and build insert query with base64encoded URL for insert into DB
        mariadb_connection = mariadb.connect(host='mydibinstance.c3odauswfp7h.us-west-1.rds.amazonaws.com', port=3306, user='root', passwd='akmalsarm12', db='my_db')
        cursor = mariadb_connection.cursor()
        insert_row = """INSERT INTO URLS (URL) VALUES ('{}')""".format(base64.urlsafe_b64encode(initial_url))
        try:
            cursor.execute(insert_row)               # Execute Insert
            mariadb_connection.commit()              # Commit statement to DB
            row_id = cursor.lastrowid                # Return last RowID for encoding
        finally:
            cursor.close()                           # Close the cursor
            mariadb_connection.close()               # Close the connection
            # If data is posted return index.html with short_url exposed, short_url = db host + base 64 encoded id
        return render_template('index.html', short_url=host + '/' + base_64_encoder(row_id))
    return render_template('index.html')             # On get return index.html withouth short_url exposed


@app.route('/<short_url>')
def link_to_url(short_url):
    """Takes short_url and extracts the db ID looks original URL up in MySQL db, redirects to original URL on click"""
    decoded_string = base_64_decoder(short_url)      # Base64 decoded string holds str rep of an int

    # Connect to DB, create cursor and build select query using decoded string
    mariadb_connection = mariadb.connect(host='mydibinstance.c3odauswfp7h.us-west-1.rds.amazonaws.com', port=3306, user='root', passwd='akmalsarm12', db='my_db')
    cursor = mariadb_connection.cursor()
    select_row = """SELECT URL FROM URLS WHERE ID= {}""".format(decoded_string)
    try:
        cursor.execute(select_row)                           # Execute query
        url_to_go = base64.urlsafe_b64decode(cursor.fetchall()[0][0].encode('utf-8'))     # fetch redirect URL
    finally:
        cursor.close()                                       # Close the cursor
        mariadb_connection.close()                           # Close the connection
    return redirect(url_to_go)                               # redirect to expanded URL

if __name__ == '__main__':                                   # Initialize Main
    check_table()                                            # Calls check_table
    app.run(debug=True)                                      # Initialize server
