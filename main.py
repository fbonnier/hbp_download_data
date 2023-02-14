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
        print ("download_data")
        print (e)
        return 1

def check_code_locations (list_of_codes):
    list_of_filenames = []
    for icode in list_of_codes:
        ifilename = str(icode["path"]) + "/" + str(icode["url"]).split("/")[-1]
        list_of_filenames.append(ifilename)
        try:
            with urllib.request.urlopen(icode["url"]) as response, open(ifilename, 'wb') as out_file:
                data = response.read() # a `bytes` object
                out_file.write(data)
        except Exception as e:
            print ("check_code_location")
            print (e)
        
    print ("Path dir :: ")
    print (os.listdir())

    code_to_return = {"url": None, "path": None}
    assert(len(list_of_codes) == len(list_of_filenames), "Code locations and filenames aren't the same size")
    for ifilename in list_of_filenames, icode in list_of_codes:
        if zipfile.is_zipfile(ifilename):
            code_to_return["path"] = ifilename
            code_to_return["url"] = icode["url"]
        elif tarfile.is_tarfile(filename):
            code_to_return["path"] = ifilename
            code_to_return["url"] = icode["url"]
        elif rarfile.is_rarfile(filename):
            code_to_return["path"] = ifilename
            code_to_return["url"] = icode["url"]
        else:
            print ("Error: code path is not a zip, rar or tar File")
            print ("Trying shutil lib")
            try:
                shutil.unpack_archive(ifilename, str(icode["path"]))
                code_to_return["path"] = icode["path"] + "/" + ifilename
                code_to_return["url"] = icode["url"]
            except Exception as e:
                print ("check_code_location_2")
                print(e)
    return code_to_return

def untar_data (path):
    try:
        json_content = []
        with tarfile.TarFile(path, 'r') as datazip:
            datazip.extractall(path)
            for iitem in datazip.namelist():
                json_content.append ({"url": "", "path": str(iitem), "hash": ""})
        return json_content
        
    except Exception as e:
        print ("untar_data")
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
        print ("unrar_data")
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
        print ("unzip_data")
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
    code = check_code_locations (json_data["Metadata"]["run"]["code"])
    # Download code
    assert(code["url"] != None)
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
            print ("Shutil failed: " + str(e))
            print ("Trying Archiver")
            os.system("arc -overwrite unarchive " + str(filename) + " " + str(code["path"]))
            # Check if the file has been correctly extracted
            print(os.listdir (str(code["path"])))

    with open("./report.json", "w") as f:
        json.dump(json_data, f, indent=4) 
    # Exit Done ?
    sys.exit()
