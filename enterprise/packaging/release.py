#!/usr/bin/python3
import sys
import yaml
import subprocess


def processMetaConfig(parsed_yaml):
    if parsed_yaml["prepareAction"] is None:
        print("No prepare action given")
    else:
        print("prepare action:")
        print("\t", parsed_yaml["prepareAction"], "\n")
        for item in parsed_yaml["prepareAction"]:
            if item.get("name") is None:
                print("No prepare action name")
            else:
                print("\t prepare action name:", item["name"], "\n")

            if item.get("script") is None:
                print("No prepare action script given")
            else:
                command = [item["script"]]
                if "topdir" in item:
                    topdir = [item["topdir"]]
                    command.extend(topdir)
                print("command: %s" % command)
                prepare_output = subprocess.check_call(
                    command)
                print(prepare_output)

    print("\t", parsed_yaml["configFiles"], "\n")
    for key, value in parsed_yaml["configFiles"].items():
        print("key:", key, "value:", value, "\n")
        with open(value, "r") as config_file:
            try:
                config_yaml = yaml.safe_load(config_file)
                processFeatureConfig(config_yaml)
            except yaml.YAMLError as err:
                print(err)
                return -1

    if parsed_yaml.get("postAction") is None:
        print("No post action given")
    else:
        print("post action:")
        print("\t", parsed_yaml["postAction"], "\n")
        for item in parsed_yaml["postAction"]:
            if item.get("name") is not None:
                print("\t post action name:", item["script"], "\n")
            if item.get("script") is not None:
                post_output = subprocess.check_output(
                    item["script"], shell=True
                    ).decode("utf-8")
                print(post_output)


def processFeatureConfig(parsed_yaml):
    print("All key/values:")
    print("\n", parsed_yaml, "\n")

    print("Build options:")
    print("\t", parsed_yaml["buildOption"], "\n")

    print("All parts:")
    print("\t", parsed_yaml["parts"], "\n")

    if parsed_yaml["parts"].get("taosd") is None:
        print("parsed_yaml['parts]['taosd'] is None")
    elif "oemName" in parsed_yaml["parts"]["taosd"]:
        print("taosd's oem name:")
        print("\t", parsed_yaml["parts"]["taosd"]["oemName"], "\n")
    else:
        print("taosd no oem name")

    if parsed_yaml["parts"].get("examples") is None:
        print("parsed_yaml['parts]['examples'] is None")
    else:
        print("examples' properties:")
        print("\t", parsed_yaml["parts"]["examples"], "\n")

    print("prepare action:")
    print("\t", parsed_yaml["prepareAction"], "\n")

    print("post action:")
    print("\t", parsed_yaml["postAction"], "\n")


def processConfig(cfg):
    with open(cfg, "r") as config_file:
        try:
            parsed_yaml = yaml.safe_load(config_file)
        except yaml.YAMLError as err:
            print(err)

    if parsed_yaml["type"] == "meta":
        processMetaConfig(parsed_yaml)
    else:
        processFeatureConfig(parsed_yaml)


def printHelp():
    print("Syntax: ./release.py <cfgFile> [version]")


if __name__ == "__main__":

    if len(sys.argv) > 2:
        print("version:", sys.argv[2], "\n")

    if len(sys.argv) > 1:
        processConfig(sys.argv[1])
    else:
        printHelp()
