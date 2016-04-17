#!/usr/bin/env python

"""
    Software Laboratory 5
    23-SZORAK
"""
import csv
from contextlib import closing

from flask import Flask, jsonify, abort, request
from datetime import datetime
import json
import cx_Oracle
import requests
import xml.etree.ElementTree as ET

app = Flask(__name__)


def xmlify(persons):
    root = ET.Element('persons')
    for person in persons:
        ET.SubElement(root, 'person', person)
    # TODO: fix UnicodeDecode Error
    return app.response_class(ET.dump(root), mimetype='application/xml')


def csvify(persons):
    # TODO: Create CSV writer
    return app.response_class(None, mimetype='text/csv')


@app.route('/persons<ext>')
def list_persons(ext):
    with get_db() as conn:
        with closing(conn.cursor()) as cur:
            cur.execute("SELECT PERSON_ID, NAME, ADDRESS FROM PERSONS")
            results = [{
                           'person_id': person_id,
                           'name': name,
                           'address': address
                       } for person_id, name, address in cur]
            # Decide return type based on extension
            if ext == '.json':
                return jsonify(persons=results)
            if ext == '.xml':
                return xmlify(results)
            if ext == '.csv':
                return csvify(results)

            # Decide return type based on Header
            accept = request.headers.get('Accept')
            if 'application/json' in accept:
                return jsonify(persons=results)
            if 'application/xml' in accept:
                return xmlify(results)
            if 'text/csv' in accept:
                return csvify(results)
            # if no matches, return error
            abort(404)


@app.route('/persons/<person_id>.json')
def person_info(person_id):
    with get_db() as conn:
        with closing(conn.cursor()) as cur:
            cur.execute("SELECT name, address, phone, income FROM PERSONS WHERE person_id = :id", id=person_id)
            result = cur.fetchone()
            if result is None:
                abort(404)
            else:
                params = {
                    'format': "json",
                    'country': "Hungary",
                    'city': result[1].split(',')[0].split('.')[0].replace('Bp', "Budapest")
                }
                r = requests.get("http://nominatim.openstreetmap.org/search", params=params)
                lat = None
                lon = None
                if r.json():
                    lat = r.json()[0]['lat']
                    lon = r.json()[0]['lon']
                return jsonify(
                    name=result[0],
                    address=result[1],
                    phone=result[2],
                    income=result[3],
                    Latitude=lat,
                    Longitude=lon,
                )


@app.route('/persons/by-address/<address>.json')
def search_address(address):
    with get_db() as conn:
        with closing(conn.cursor()) as cur:
            cur.execute(
                "SELECT PERSON_ID, NAME, ADDRESS FROM PERSONS WHERE LOWER(ADDRESS) LIKE :a"
                , a='%' + address.replace('%', '{%}').lower() + '%'
            )
            results = [{
                           'person_id': person_id,
                           'name': name,
                           'address': address
                       } for person_id, name, address in cur]
            return jsonify(persons=results)


@app.route('/persons/by-name/<name>.json')
def search_name(name):
    with get_db() as conn:
        with closing(conn.cursor()) as cur:
            cur.execute(
                "SELECT PERSON_ID, NAME, ADDRESS FROM PERSONS WHERE LOWER(NAME) LIKE :n"
                , n='%' + name.replace('%', '{%}').lower() + '%'
            )
            results = [{
                           'person_id': person_id,
                           'name': name,
                           'address': address
                       } for person_id, name, address in cur]
            return jsonify(persons=results)


@app.route('/szemelyek.json')
def list_people():
    """Lists the first 50 persons in the database"""
    conn = get_db()
    try:
        cur = conn.cursor()
        try:
            # this table has 10k rows, so we intentionally limit the result set to 50
            # (Oracle note: not the first 50 rows by name, but rather
            # the first 50 rows of the table, which are then ordered by name)
            # also, long queries can be broken into two shorter lines like this
            cur.execute('''SELECT szemelyi_szam, nev FROM oktatas.szemelyek
                WHERE ROWNUM < 50 ORDER BY nev ASC''')
            # there's a better way, but outside the scope of this lab:
            # http://docs.python.org/2/tutorial/datastructures.html#list-comprehensions
            results = []
            # we make use of the fact that
            #  - cursors are iterable and
            #  - `for` can unpack objects returned by each iteration
            for szemelyi_szam, nev in cur:
                results.append({'szemelyi_szam': szemelyi_szam, 'nev': nev})
            return jsonify(szemelyek=results)
        finally:
            cur.close()
    finally:
        # this is also a naive implementation, a more Pythonic solution:
        # http://docs.python.org/2/library/contextlib.html#contextlib.closing
        conn.close()


@app.route('/szemely/<szemelyi_szam>.json')
def show_person(szemelyi_szam):
    """Shows the details of a single person by szemelyi_szam"""
    conn = get_db()
    try:
        cur = conn.cursor()
        try:
            cur.execute('SELECT nev FROM oktatas.szemelyek WHERE szemelyi_szam = :sz',
                        sz=szemelyi_szam)
            # fetchone() returns a single row if there's one, otherwise None
            result = cur.fetchone()
            # in Python '==' compares by value, 'is' compares by reference
            # (of course, former would work too, but it's slower and unnecessary)
            # 'None' is the Python version of null, it's a singleton object, so
            # we can safely compare to it using 'is' (Java/C#: result == null)
            if result is None:
                # no rows -> 404 Not Found (no need to return manually)
                abort(404)
            else:
                links = []
                try:
                    # we query the Wikipedia API to see what happened the day
                    # the person was born based on szemelyi_szam
                    born = datetime.strptime(szemelyi_szam[1:7], '%y%m%d')
                    params = {
                        'action': 'query',
                        # 2012-04-01 -> "April 01" -> "April 1"
                        'titles': born.strftime('%B %d').replace('0', ''),
                        'prop': 'extlinks',
                        'format': 'json',
                    }
                    # API docs: http://www.mediawiki.org/wiki/API:Tutorial
                    # Example for 1st April:
                    # https://en.wikipedia.org/w/api.php?action=query&format=json&prop=extlinks&titles=April%201
                    res = requests.get('https://en.wikipedia.org/w/api.php', params=params)
                    for page in res.json()['query']['pages'].itervalues():
                        for link in page['extlinks']:
                            for href in link.itervalues():
                                links.append(href)
                except:
                    pass  # necessary if a clause would be empty in Python

                # result set rows can be indexed too
                return jsonify(nev=result[0], links=links)
        finally:
            cur.close()
    finally:
        conn.close()


@app.route('/datetest.json')
def date_test():
    conn = get_db()
    try:
        cur = conn.cursor()
        try:
            # http://www.oracle.com/technetwork/articles/dsl/prez-python-timesanddates-093014.html
            # https://docs.python.org/2/library/datetime.html
            # it's casted automatically to datetime
            cur.execute('SELECT datum, usd FROM oktatas.mnb_deviza WHERE id < 10')
            results = []
            for datum, usd in cur:
                results.append({'datum': datum, 'datum_iso': datum.isoformat(), 'usd': usd})
            return jsonify(arfolyamok=results)
        finally:
            cur.close()
    finally:
        conn.close()


@app.route('/verbtest.json', methods=['PUT', 'POST'])
def verb_test():
    """Lets you test HTTP verbs different from GET, expects and returns data in JSON format"""
    # it also shows you how to access the method used and the decoded JSON data
    return jsonify(method=request.method, data=request.json, url=request.url)


def get_db():
    """Connects to the RDBMS and returns a connection object"""
    # when used with a `file` object, `with` ensures it gets closed
    with file('config.json') as config_file:
        config = json.load(config_file)
    return cx_Oracle.connect(config['user'], config['pass'], config['host'])


if __name__ == "__main__":
    import os

    os.environ['NLS_LANG'] = '.UTF8'
    app.run(debug=True, port=os.getuid() + 10000)
