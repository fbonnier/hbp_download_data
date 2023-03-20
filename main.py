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
import re

def download_data (url: str, filepath: str):
    
    try:
        with urllib.request.urlopen(url) as response, open(filepath, 'wb') as out_file:
            data = response.read() # a `bytes` object
            out_file.write(data)
    except Exception as e:
        print ("download_data")
        print (e)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Download and extract data from HBP metadata JSON file. Only code will be extracted")

    parser.add_argument("--json", type=argparse.FileType('r'), metavar="JSON Metadata file", nargs=1, dest="json", default="",\
    help="JSON File that contains Metadata of the HBP model to run")

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
    # Download workflow runfile
    if workflow_run_file["url"] and workflow_run_file["path"]:
        download_data(workflow_run_file["url"], workflow_run_file["path"])
    # Download workflow datafile
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
    # Download code
    for icode in json_data["Metadata"]["run"]["code"]:
        assert(icode["url"] != None)

        if icode["url"] and icode["filepath"]:
            download_data(url=icode["url"], filepath=icode["filepath"])
        try:
            # Unpack code to run
            shutil.unpack_archive(icode["filepath"], icode["path"])
        except Exception as e:
            print ("Shutil failed: " + str(e))
            print ("Trying Archiver")
            os.system("arc -overwrite unarchive " + icode["filepath"] + " " + icode["path"])

        # try:
        #     # Unpack control code as outputs
        #     shutil.unpack_archive(icode["filepath"], json_data["Metadata"]["workdir"] + "/outputs/" + icode["filepath"].)
        # except Exception as e:
        #     print ("Shutil failed: " + str(e))
        #     print ("Trying Archiver")
        #     os.system("arc -overwrite unarchive " + icode["filepath"] + " " + json_data["Metadata"]["workdir"] + "/outputs/" + icode["filepath"])

        # Add all files of code as potential outputs/results
        try:
            for current_dir, subdirs, files in os.walk( icode["path"] ):
                for filename in files:
                    relative_path = os.path.join( current_dir, filename )
                    absolute_path = os.path.abspath( relative_path )
                    json_data["Metadata"]["run"]["outputs"].append({"url": None,  "path": str(absolute_path), "hash": ""})
                    print (absolute_path)
        except Exception as e:
            print (e)


    with open("./report.json", "w") as f:
        json.dump(json_data, f, indent=4) 
    # Exit Done ?
    sys.exit()
