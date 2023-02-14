import os
import sys
import argparse
import json as json
import warnings
import urllib.request
import zipfile
import tarfile
import rarfile
import shutil

def download_data (url, path):

    filename =  str(path) + "/" + str(str(url).split("/")[-1])
    try:
        with urllib.request.urlopen(url) as response, open(filename, 'wb') as out_file:
            data = response.read() # a `bytes` object
            out_file.write(data)
        print ("Path dir :: ")
        print (os.listdir(path))
        return 0
    except Exception as e:
        print (e)
        return 1


def untar_data (path):
    try:
        json_content = []
        with tarfile.TarFile(path, 'r') as datazip:
            datazip.extractall(path)
            for iitem in datazip.namelist():
                json_content.append ({"url": "", "path": str(iitem), "hash": ""})
        return json_content
        
    except Exception as e:
        print (e)
        return 1

def unrar_data (path):
    try:
        json_content = []
        with rarfile.RarFile(path, 'r') as datazip:
            datazip.extractall(path)
            for iitem in datazip.namelist():
                json_content.append ({"url": "", "path": str(iitem), "hash": ""})
        return json_content
        
    except Exception as e:
        print (e)
        return 1

def unzip_data (path):
    try:
        json_content = []
        with zipfile.ZipFile(path, 'r') as datazip:
            datazip.extractall(path)
            for iitem in datazip.namelist():
                json_content.append ({"url": "", "path": str(iitem), "hash": ""})
        return json_content
        
    except Exception as e:
        print (e)
        return 1

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Download and extract data from HBP metadata JSON file. Only code will be extracted")

    parser.add_argument("--json", type=argparse.FileType('r'), metavar="JSON Metadata file", nargs=1, dest="json", default="",\
    help="JSON File that contains Metadata of the HBP model to run")

    # parser.add_argument("--kg", type=int, metavar="KG Version", nargs=1, dest="kg", default=int(os.environ.get("KG_VERSION", 2)),\
    # help="Version number of Knowledge Graph to use")

    args = parser.parse_args()

    # Load JSON data
    json_file = args.json[0]
    if not json_file:
        print ("Fatal Error:  Invalid JSON File, please give a valid JSON file using \"--json <path-to-file>\"")
        exit(1)
    json_data = json.load(json_file)

    # Load workdir
    workdir = json_data["Metadata"]["workdir"]

    # Load workflow
    workflow_run_file = json_data["Metadata"]["workflow"]["run"]
    workflow_data_file = json_data["Metadata"]["workflow"]["data"]
    # Download workflow
    if workflow_run_file["url"] and workflow_run_file["path"]:
        download_data(workflow_run_file["url"], workflow_run_file["path"])
    if workflow_data_file["url"] and workflow_data_file["path"]:
        download_data(workflow_data_file["url"], workflow_data_file["path"])
    
    # Load inputs
    inputs = json_data["Metadata"]["run"]["inputs"]
    # Download inputs
    for iinput in inputs:
        if iinput["url"] and iinput["path"]:
            download_data(iinput["url"], iinput["path"])

    # Load outputs
    outputs = json_data["Metadata"]["run"]["outputs"]
    # Download outputs
    for ioutput in outputs:
        if ioutput["url"] and ioutput["path"]:
            download_data(ioutput["url"], ioutput["path"])

    # Load code
    code = { "url": json_data["Metadata"]["run"]["url"], "path": json_data["Metadata"]["run"]["path"]}
    # Download code
    if code["url"] and code["path"]:
        download_data(code["url"], code["path"])
        
    # Unzip code
    # Update and write JSON report including files in archive as outputs potentials
    filename =  str(code["path"]) + "/" + str(str(code["url"]).split("/")[-1])
    print ("Filename :: " + str(filename))
    if zipfile.is_zipfile(filename):
        json_data["Metadata"]["run"]["outputs"].append(unzip_data(filename))
    elif tarfile.is_tarfile(filename):
        json_data["Metadata"]["run"]["outputs"].append(untar_data(filename))
    elif rarfile.is_rarfile(filename):
        json_data["Metadata"]["run"]["outputs"].append(unrar_data(filename))
    else:
        print ("Error: code path is not a zip, rar or tar File")
        print ("Trying shutil lib")
        try:
            shutil.unpack_archive(filename, str(code["path"]))
        except Exception as e:
            print (e)

    with open("./report.json", "w") as f:
        json.dump(json_data, f, indent=4) 
    # Exit Done ?
    sys.exit()
