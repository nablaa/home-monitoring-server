#!/usr/bin/env python

import logging as log
import argparse
import rrdtool
import RRDtool
import os.path
import json
import monitoring

IMAGE_WIDTH = 800
IMAGE_HEIGHT = 400
IMAGE_NAMES_MAPPING = {
    "hour.png": "-6h",
    "day.png": "-1d",
    "week.png": "-1w",
    "month.png": "-1m",
    "year.png": "-1y",
}


def main():
    args = parse_args()
    init_logging(args.log_level)
    config_dict = json.load(args.config_filename)
    output_graphs(config_dict, args.output_dir)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", dest="log_level", default="WARNING",
                        help="Log level (DEBUG, INFO, WARNING or ERROR)")
    parser.add_argument("-o", "--output-dir", dest="output_dir", required=True,
                        help="Directory where output images are put")
    parser.add_argument("config_filename",
                        metavar="CONFIG-FILENAME",
                        type=argparse.FileType('r'))
    return parser.parse_args()


def init_logging(log_level):
    log.basicConfig(level=log_level,
                    format="%(asctime)s - %(levelname)s - %(message)s")


def output_graphs(config, output_dir):
    if not os.path.isdir(output_dir):
        log.info("Creating output directory: %s", output_dir)
        os.makedirs(output_dir)

    unit_label = "Degrees (C)"
    draw_graphs(config, output_dir, unit_label)

    rrd_filename = config["temperature-rrd"]
    rrd_info = RRDtool.RRD(rrd_filename).info()
    ds_names = monitoring.get_data_source_names_from_info(rrd_info)

    for name in ds_names:
        for image_name, time_range in IMAGE_NAMES_MAPPING.items():
            draw_detailed_graph_for_dataset(output_dir, name, rrd_filename, name,
                                            image_name, time_range, unit_label)


def draw_graphs(config, output_dir, label):
    log.debug("Drawing graphs")
    rrd_filename = config["temperature-rrd"]
    rrd_info = RRDtool.RRD(rrd_filename).info()
    ds_names = monitoring.get_data_source_names_from_info(rrd_info)
    defs = get_defs(rrd_filename, ds_names)
    colors = ["#FF531A", "#4D79FF", "#1C800F", "#999999", "#FFCC00"]
    lines = get_lines(rrd_filename, ds_names, colors)
    texts = get_texts(rrd_filename, ds_names)

    for image, start in IMAGE_NAMES_MAPPING.items():
        draw_graph(rrd_filename, os.path.join(output_dir, image), start, label,
                   IMAGE_WIDTH, IMAGE_HEIGHT, defs, lines, texts)


def draw_detailed_graph_for_dataset(output_dir, name, rrd_filename, dataset_name,
                                    image_filename, start, unit_label):
    defs = get_defs(rrd_filename, [dataset_name])
    colors = ["#FF531A", "#4D79FF", "#1C800F", "#999999", "#FFCC00"]
    lines = get_lines(rrd_filename, [dataset_name], colors)
    texts = get_texts(rrd_filename, [dataset_name])
    images_dir = os.path.join(output_dir, "detailed")

    image_filepath = os.path.join(images_dir, dataset_name, image_filename)
    draw_graph(rrd_filename, image_filepath, start, unit_label,
               IMAGE_WIDTH, IMAGE_HEIGHT, defs, lines, texts)


def draw_graph(rrd_filename, filepath, start, label, width, height,
               defs, lines, texts):
    log.debug("Drawing graph '%s' for '%s'", filepath, rrd_filename)

    directory = os.path.dirname(filepath)
    if not os.path.exists(directory):
        os.makedirs(directory)

    rrdtool.graph(filepath, "--start", start,
                  "--vertical-label", label,
                  "--width", str(width),
                  "--height", str(height),
                  "--alt-autoscale",
                  "--grid-dash", "1:0",
                  "--dynamic-labels",
                  defs,
                  lines,
                  texts)


def get_defs(rrd_filename, ds_names):
    defs = []
    log.debug("Getting DEFs for '%s'", rrd_filename)
    for name in ds_names:
        defs.append("DEF:%s=%s:%s:AVERAGE" % (name, rrd_filename, name))

    log.debug("DEFs: %s", defs)
    return defs


def get_detailed_defs(rrd_filename, ds_names):
    defs = []
    log.debug("Getting detailed DEFs for '%s'", rrd_filename)
    for name in ds_names:
        defs.append("DEF:%s=%s:%s:AVERAGE" % (name, rrd_filename, name))
        defs.append("VDEF:%s_min=%s,MINIMUM" % (name, name))
        defs.append("VDEF:%s_max=%s,MAXIMUM" % (name, name))
        defs.append("VDEF:%s_last=%s,LAST" % (name, name))
        defs.append("VDEF:%s_average=%s,AVERAGE" % (name, name))

    log.debug("Detailed DEFs: %s", defs)
    return defs


def get_lines(rrd_filename, ds_names, colors):
    lines = []
    log.debug("Getting LINEs for '%s'", rrd_filename)
    for name, color in zip(ds_names, colors):
        # TODO label
        lines.append("LINE2:%s%s:%s" % (name, color, name))

    log.debug("LINEs: %s", lines)
    return lines


def get_detailed_lines(rrd_filename, ds_names):
    lines = []
    log.debug("Getting detailed LINEs for '%s'", rrd_filename)
    for name in ds_names:
        lines.append("LINE1:%s#444444:Actual" % name)
        lines.append("LINE1:%s_min#0000FF:Minimum" % name)
        lines.append("LINE1:%s_average#00FF00:Average" % name)
        lines.append("LINE1:%s_max#FF0000" % name)
        lines.append("LINE1:%s_average#00FF00" % name)

    log.debug("Detailed LINEs: %s", lines)
    return lines


def get_texts(rrd_filename, ds_names):
    texts = []
    log.debug("Getting texts for '%s'", rrd_filename)
    # TODO label
    longest_label_length = max(len(name) for name in ds_names)

    for name in ds_names:
        # TODO label
        label_length = len(name)
        padding = " " * (longest_label_length - label_length)
        # TODO label
        texts.append("GPRINT:%s:LAST:%s%s   current\: %%3.2lf" %
                     (name, name, padding))
        texts.append("GPRINT:%s:MIN: min\: %%3.2lf" % name)
        texts.append("GPRINT:%s:MAX: max\: %%3.2lf" % name)
        texts.append("GPRINT:%s:AVERAGE: average\: %%3.2lf\\n" % name)

    log.debug("Texts: %s", texts)
    return texts


if __name__ == "__main__":
    main()
