#!/usr/bin/env python2

import json
import argparse
import logging as log
import os.path
import RRDtool
import requests
import subprocess
import tempfile
import sys
import time
from lxml import etree as ET


def main():
    args = parse_args()
    init_logging(args.log_level)
    config_dict = json.load(args.config_filename)
    run_monitoring_server(config_dict)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", dest="log_level", default="WARNING",
                        help="Log level (DEBUG, INFO, WARNING or ERROR)")
    parser.add_argument("config_filename",
                        metavar="CONFIG-FILENAME",
                        type=argparse.FileType('r'))
    return parser.parse_args()


def init_logging(log_level):
    log.basicConfig(level=log_level,
                    format="%(asctime)s - %(levelname)s - %(message)s")


def run_monitoring_server(config):
    log.info("Starting monitoring server")
    interval = config["polling-interval-seconds"]
    log.info("Polling every %d seconds", interval)

    while True:
        temperature_datas = loop_temperature_servers(config)
        if temperature_datas is not {}:
            update_data_to_rrd(config["temperature-rrd"], config["rras"],
                               temperature_datas)
        log.debug("Update done")
        time.sleep(interval)


def loop_temperature_servers(config):
    log.debug("Reading temperature data from servers")
    temperature_datas = {}
    for server in config["servers"]:
        hostname = server["hostname"]
        port = server["port"]
        try:
            temperature_datas.update(read_server_temperature_data(hostname, port))
        except requests.exceptions.ConnectionError as e:
            log.warning("Could not connect to temperature server (%s:%d): '%s'", hostname, port, e)
    return temperature_datas


def read_server_temperature_data(hostname, port):
    url = "http://" + hostname + ":" + str(port) + "/temperatures"
    log.debug("Querying temperatures from: %s", url)
    r = requests.get(url)
    if r.status_code != 200:
        log.warning("HTTP query to '%s' returned error: %d", url, r.status_code)
        return {}
    return r.json()


def update_data_to_rrd(filename, rras, data):
    log.debug("Updating data to RRD")
    data_source_names = data.keys()
    rrd = open_or_create_rrd_database_if_not_existing(filename, rras, data_source_names)
    was_added = add_missing_data_sources(filename, rrd, data_source_names)
    if was_added:
        # We need to reopen the RRD database as adding was done by creating new RRD file
        log.debug("Re-opening RRD after data sources were added")
        rrd = open_or_create_rrd_database_if_not_existing(filename, rras, data_source_names)

    add_datapoints_to_rrd(rrd, data)


def open_or_create_rrd_database_if_not_existing(filename, rras, data_source_names):
    if not os.path.exists(filename):
        return create_rrd_database(filename, rras, data_source_names)
    else:
        log.debug("Using existing RRD database file: %s", filename)
        return RRDtool.RRD(filename)


def create_rrd_database(filename, rras, data_source_names):
    log.info("Creating empty RRD: %s", filename)
    rra_string = get_rra_string(rras)
    ds_string = get_dataset_string(data_source_names)

    rrd = RRDtool.create(filename, "--start", "now", "--step", "300",
                         rra_string, ds_string)

    log.info("RRD created")
    return rrd


def get_rra_string(rra_configs):
    rras = []
    for rra in rra_configs:
        log.debug("RRA for '%s'", rra)
        rras.append("RRA:%(type)s:%(xff)s:%(steps)s:%(rows)s" % rra)

    log.debug("RRAs are: %s", rras)
    return rras


def get_dataset_string(data_source_names):
    datasets = []
    for dataset_name in data_source_names:
        log.debug("Creating dataset '%s'", dataset_name)
        dataset = {"name": dataset_name, "type": "GAUGE",
                   "timeout": 600, "min": -100, "max": 100}
        datasets.append("DS:%(name)s:%(type)s:%(timeout)s:%(min)s:%(max)s"
                        % dataset)

    log.debug("Datasets are: %s", datasets)
    return datasets


def add_missing_data_sources(filename, rrd, data_source_names):
    missing_data_sources = get_missing_data_source_names(rrd, data_source_names)
    if len(missing_data_sources) > 0:
        add_data_sources_to_rrd(filename, missing_data_sources)
        return True
    return False


def get_missing_data_source_names(rrd, data_source_names):
    rrd_info = rrd.info()
    return set(data_source_names) - set(get_data_source_names_from_info(rrd_info))


def get_data_source_names_from_info(rrd_info):
    """
    >>> info = {
    ...         'ds[mytemp1].index': 1L,
    ...         'ds[mytemp1].last_ds': u'U',
    ...         'ds[mytemp1].minimal_heartbeat': 600L,
    ...         'ds[mytemp1].type': u'GAUGE',
    ...         'ds[mytemp1].unknown_sec': 167L,
    ...         'ds[mytemp2].index': 0L,
    ...         'ds[mytemp2].last_ds': u'U',
    ...         'ds[mytemp2].minimal_heartbeat': 600L,
    ...         'ds[mytemp2].type': u'GAUGE',
    ...         'ds[mytemp2].unknown_sec': 167L,
    ...         'filename': u'temperatures.rrd',
    ...         'header_size': 2048L,
    ...         'last_update': 1437480467L,
    ...         'rra[0].cdp_prep[0].unknown_datapoints': 0L,
    ...         'rra[0].cdp_prep[0].value': None,
    ...         'rra[0].cdp_prep[1].unknown_datapoints': 0L,
    ...         'rra[0].cdp_prep[1].value': None,
    ...         'rra[0].cf': u'AVERAGE',
    ...         'rra[0].cur_row': 442966L,
    ...         'rra[0].pdp_per_row': 1L,
    ...         'rra[0].rows': 525600L,
    ...         'rra[1].cdp_prep[0].unknown_datapoints': 0L,
    ...         'rra[1].cdp_prep[0].value': None,
    ...         'rra[1].cdp_prep[1].unknown_datapoints': 0L,
    ...         'rra[1].cdp_prep[1].value': None,
    ...         'rra[1].cf': u'AVERAGE',
    ...         'rra[1].cur_row': 244L,
    ...         'rra[1].pdp_per_row': 1L,
    ...         'rra[1].rows': 288L,
    ...         'rra[2].cdp_prep[0].unknown_datapoints': 1L,
    ...         'rra[2].cdp_prep[0].value': None,
    ...         'rra[2].cdp_prep[1].unknown_datapoints': 1L,
    ...         'rra[2].cdp_prep[1].value': None,
    ...         'rra[2].cf': u'AVERAGE',
    ...         'rra[2].cur_row': 126L,
    ...         'rra[2].pdp_per_row': 12L,
    ...         'rra[2].rows': 168L,
    ...         'rra[3].cdp_prep[0].unknown_datapoints': 1L,
    ...         'rra[3].cdp_prep[0].value': None,
    ...         'rra[3].cdp_prep[1].unknown_datapoints': 1L,
    ...         'rra[3].cdp_prep[1].value': None,
    ...         'rra[3].cf': u'AVERAGE',
    ...         'rra[3].cur_row': 573L,
    ...         'rra[3].pdp_per_row': 12L,
    ...         'rra[3].rows': 720L,
    ...         'rra[4].cdp_prep[0].unknown_datapoints': 145L,
    ...         'rra[4].cdp_prep[0].value': None,
    ...         'rra[4].cdp_prep[1].unknown_datapoints': 145L,
    ...         'rra[4].cdp_prep[1].value': None,
    ...         'rra[4].cf': u'AVERAGE',
    ...         'rra[4].cur_row': 169L,
    ...         'rra[4].pdp_per_row': 288L,
    ...         'rra[4].rows': 365L,
    ...         'rrd_version': u'0003',
    ...         'step': 300L,
    ...         }
    >>> get_data_source_names_from_info(info)
    ['mytemp2', 'mytemp1']
    """
    names = {}
    for field in rrd_info.keys():
        if field.startswith("ds[") and field.endswith("].index"):
            name = field.split("ds[")[1].split("].index")[0]
            names[name] = rrd_info[field]

    return [n for n, index in sorted(names.items(), key=lambda x: x[1])]


def add_data_sources_to_rrd(filename, data_sources):
    log.debug("Adding missing data sources to RRD: %s", list(data_sources))
    tmp_xml = dump_rrd_to_temp_file(filename)
    tmp_xml = add_data_sources_to_rrd_xml_file(tmp_xml, data_sources)
    restore_rrd_from_xml(filename, tmp_xml)
    log.debug("Data sources added")


def dump_rrd_to_temp_file(rrd_filename):
    tmp_xml = tempfile.TemporaryFile()
    rval = subprocess.call(["rrdtool", "dump", rrd_filename], stdout=tmp_xml)
    if rval != 0:
        log.error("rrdtool dump returned: %d", rval)
        sys.exit(1)

    tmp_xml.seek(0)
    return tmp_xml


def restore_rrd_from_xml(rrd_filename, tmp_xml):
    log.debug("Restoring XML back to RRD")
    rval = subprocess.call(["rrdtool", "restore", "--force-overwrite", tmp_xml.name, rrd_filename])
    if rval != 0:
        log.error("rrdtool restore returned: %d", rval)
        sys.exit(1)


def add_data_sources_to_rrd_xml_file(rrd_xml_file, data_sources):
    log.debug("Parsing RRD XML")
    xml = ET.parse(rrd_xml_file)

    ds_elems = []
    for data_source_name in data_sources:
        ds = ET.Element("ds")
        name = ET.SubElement(ds, "name")
        ds_type = ET.SubElement(ds, "type")
        minimal_heartbeat = ET.SubElement(ds, "minimal_heartbeat")
        ds_min = ET.SubElement(ds, "min")
        ds_max = ET.SubElement(ds, "max")
        last_ds = ET.SubElement(ds, "last_ds")
        value = ET.SubElement(ds, "value")
        unknown_sec = ET.SubElement(ds, "unknown_sec")

        name.text = data_source_name
        ds_type.text = "GAUGE"
        minimal_heartbeat.text = "600"
        ds_min.text = "-1.0000000000e+02"
        ds_max.text = "1.0000000000e+02"
        last_ds.text = "U"
        value.text = "0.0000000000e+00"
        unknown_sec.text = "167"
        ds_elems.append(ds)

    root = xml.getroot()
    rra_index = root.index(root.find("rra"))

    log.debug("Inserting <ds> elements")
    for ds_elem in ds_elems:
        root.insert(rra_index, ds_elem)

    rra_elems = root.findall("rra")
    for rra_elem in rra_elems:
        cdp_prep_elem = rra_elem.find("cdp_prep")

        log.debug("Inserting <ds> elements in <cdp_prep>")
        for data_source_name in data_sources:
            ds_elem = ET.Element("ds")
            primary_value = ET.SubElement(ds_elem, "primary_value")
            secondary_value = ET.SubElement(ds_elem, "secondary_value")
            value = ET.SubElement(ds_elem, "value")
            unknown_datapoints = ET.SubElement(ds_elem, "unknown_datapoints")

            primary_value.text = "NaN"
            secondary_value.text = "NaN"
            value.text = "NaN"
            unknown_datapoints.text = "0"
            cdp_prep_elem.append(ds_elem)

        database_elem = rra_elem.find("database")

        log.debug("Adding <v> elements to database")
        for row in database_elem.findall("row"):
            for data_source_name in data_sources:
                v = ET.Element("v")
                v.text = "NaN"
                row.append(v)

    tmp_xml = tempfile.NamedTemporaryFile()
    xml.write(tmp_xml)
    tmp_xml.seek(0)
    return tmp_xml


def add_datapoints_to_rrd(rrd, datapoints):
    log.debug("Adding data '%s' to RRD", datapoints)
    rrd_info = rrd.info()
    ds_names = get_data_source_names_from_info(rrd_info)

    values = []
    for ds_name in ds_names:
        if ds_name in datapoints:
            values.append("%s" % datapoints[ds_name])
        else:
            values.append("U")

    value_str = "N:" + ":".join(values)
    log.debug("Updating RRD with Value str: %s", value_str)
    rrd.update([value_str])


if __name__ == "__main__":
    main()
