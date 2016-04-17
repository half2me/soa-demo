#!/usr/bin/env python

"""
    Software Laboratory 5
    23-SZORAK
"""
import StringIO
import csv
from contextlib import closing

from flask import Flask, jsonify, abort, request
from datetime import datetime
import json
import cx_Oracle
import requests
import xml.etree.ElementTree as ET

app = Flask(__name__)


def xmlify(data):
    """
    Encodes data into XML
    :param data: data to encode into XML
    :return: Flask response
    """
    root = ET.Element('persons')
    for person in data:
        ET.SubElement(root, 'person', {
            'person_id': str(person['person_id']),
            'name': person['name'],
            'address': person['address']
        })
    return app.response_class(ET.tostring(root), mimetype='application/xml')


def csvify(data):
    """
    Encodes data into CSV
    :param data: data to encode into CSV
    :return: Flask response
    """
    output = StringIO.StringIO()
    csvwriter = csv.writer(output)

    # headers
    csvwriter.writerow(['person_id', 'name', 'address'])

    # data
    for person in data:
        csvwriter.writerow([
            str(person['person_id']),
            person['name'],
            person['address']
        ])
    return app.response_class(output.getvalue(), mimetype='text/csv')


@app.route('/persons<ext>')
def list_persons(ext):
    """
    list all persons in the db
    :param ext: extension (json, xml, csv)
    :return: Flask response
    """
    with get_db() as conn:
        with closing(conn.cursor()) as cur:
            cur.execute("SELECT PERSON_ID, NAME, ADDRESS FROM PERSONS")
            results = [{
                           'person_id': person_id,
                           'name': name.decode('utf-8'),
                           'address': address.decode('utf-8')
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
    """
    Detailed info about a person
    :param person_id: id of the person
    :return: Flask response
    """
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
    """
    Search by address
    :param address: address
    :return: Flask Response
    """
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
    """
    Search by name
    :param name: name
    :return: Flask Response
    """
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
