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

def download_data (url, filepath):
    
    try:
        with urllib.request.urlopen(url) as response, open(filepath, 'wb') as out_file:
            data = response.read() # a `bytes` object
            out_file.write(data)
        return 0
    except Exception as e:
        print ("download_data")
        print (e)
        return 1

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
    filename =  str(workflow_run_file["path"]) + "/" + str(str(workflow_run_file["url"]).split("/")[-1])
    if workflow_run_file["url"] and filename:
        download_data(workflow_run_file["url"], filename)
    # Download workflow datafile
    filename =  str(workflow_data_file["path"]) + "/" + str(str(workflow_data_file["url"]).split("/")[-1])
    if workflow_data_file["url"] and filename:
        download_data(workflow_data_file["url"], filename)
    
    # Load inputs
    inputs = json_data["Metadata"]["run"]["inputs"]
    # Download inputs
    for iinput in inputs:
        filename =  str(iinput["path"]) + "/" + str(str(iinput["url"]).split("/")[-1])
        if iinput["url"] and filename:
            download_data(iinput["url"], filename)

    # Load outputs
    outputs = json_data["Metadata"]["run"]["outputs"]
    # Download outputs
    for ioutput in outputs:
        filename =  str(ioutput["path"]) + "/" + str(str(ioutput["url"]).split("/")[-1])
        if ioutput["url"] and filename:
            download_data(ioutput["url"], filename)

    # Load code
    # Download code
    for icode in json_data["Metadata"]["run"]["code"]:
        assert(icode["url"] != None)
        if icode["url"] and icode["filepath"]:
            download_data(url=icode["url"], filepath=icode["filepath"])
        
        # Unzip code
        if zipfile.is_zipfile(icode["filepath"]):
            # Rename file to add extension
            os.rename(icode["filepath"], icode["filepath"] + ".zip")
            icode["filepath"] = icode["filepath"] + ".zip"
        elif tarfile.is_tarfile(icode["filepath"]):
            # Rename file to add extension
            os.rename(icode["filepath"], icode["filepath"] + ".tar")
            icode["filepath"] = icode["filepath"] + ".tar"
        elif rarfile.is_rarfile(icode["filepath"]):
            # Rename file to add extension
            os.rename(icode["filepath"], icode["filepath"] + ".rar")
            icode["filepath"] = icode["filepath"] + ".rar"

        try:
            shutil.unpack_archive(icode["filepath"], icode["path"])
        except Exception as e:
            print ("Shutil failed: " + str(e))
            print ("Trying Archiver")
            os.system("arc -overwrite unarchive " + icode["filepath"] + " " + icode["path"])
            # Check if the file has been correctly extracted
            print(os.listdir (str(icode["path"])))

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
