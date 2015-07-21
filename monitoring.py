#!/usr/bin/env python2

import json
import argparse
import logging as log
import os.path
import RRDtool
import requests


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

    # TODO loop with interval from argparse
    temperature_datas = loop_temperature_servers(config)
    if temperature_datas is not {}:
        update_data_to_rrd(config["temperature-rrd"], config["rras"],
                           temperature_datas)


def loop_temperature_servers(config):
    temperature_datas = {}
    for server in config["servers"]:
        hostname = server["hostname"]
        port = server["port"]
        temperature_datas.update(read_server_temperature_data(hostname, port))
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
    # TODO add missing data sources if necessary
    # TODO add data


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


if __name__ == "__main__":
    main()
