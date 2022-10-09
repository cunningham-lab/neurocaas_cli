## core functions to run analysis behavior. 
import boto3
import os
import json
import time
import polling2
import logging
from .Interface_S3 import upload,download

s3_resource = boto3.resource("s3")
s3_client = boto3.client("s3")

## util functions 
def bucket_prefix_to_fullpath(b,p):
    """convert a bucket and prefix to a full s3 path

    """
    return "s3://{}/{}".format(b,p)

def fullpath_to_bucket_prefix(s):
    """convert a full s3 path to bucket and prefix. 

    """
    bucketname, keyname = s.split("s3://")[-1].split("/",1)
    return bucketname,keyname

def ls_name(bucket_name, path, exclude_folder = False):
    """TODO: Only used for polling: use for all others too. Get the names of all objects in bucket under a given prefix path as strings. Takes the name of the bucket as input, not hte bucket itself for usage outside of the utils module.  
    
    :param bucket_name: name of s3 bucket to list. 
    :type bucket_name: str
    :param path: prefix path specifying the location you want to list. 
    :type path: str
    :return: A list of strings describing the objects under the specified path in the bucket. 
    :rtype: list of strings
    """
    bucket = s3_resource.Bucket(bucket_name)
    if exclude_folder == False:
        all_files = [
            objname.key for objname in bucket.objects.filter(Prefix=path) 
        ]
    else:
        all_files = [
            objname.key for objname in bucket.objects.filter(Prefix=path) if path != objname.key
        ]
    return all_files

    
## main functions
def upload_data(b,g,datapath):
    """Given the bucket name, group name, and path to a local file, upload it to NeuroCAAS as data. 

    """
    response = {"uploaded_file_path":None,"errors":None}
    try:
        upload(datapath,bucket_prefix_to_fullpath(b,os.path.join(g,"inputs",os.path.basename(datapath))))
        response["uploaded_file_path"] = datapath
    except AssertionError:    
        response["errors"] = "path misformatted."
    except OSError:    
        response["errors"] = "file does not exist."
    return response    
        

def upload_config(b,g,configpath):    
    """Given the bucket name, group name, and path to a local file, upload it to NeuroCAAS as config. 

    """
    response = {"uploaded_file_path":None,"errors":None}
    try:
        upload(configpath,bucket_prefix_to_fullpath(b,os.path.join(g,"configs",os.path.basename(configpath))))
        response["uploaded_file_path"] = configpath
    except AssertionError:    
        response["errors"] = "path misformatted."
    except OSError:    
        response["errors"] = "file does not exist."
    return response    

def list_inputs(b,g):
    """Given the bucket name and group name, return the data fiels and config files currently associated. 

    """
    bucket = s3_resource.Bucket(b)
    data = ls_name(b,os.path.join(g,"inputs/"),exclude_folder = True) #[objname.key for objname in bucket.objects.filter(Prefix=os.path.join(g,"inputs/")) if objname.key is not os.path.join(g,"inputs/")]
    configs = [objname.key for objname in bucket.objects.filter(Prefix=os.path.join(g,"configs/")) if objname.key is not os.path.join(g,"configs/")]
    configs = ls_name(b,os.path.join(g,"configs/"),exclude_folder = True)
    return data, configs
    
def list_results(b,g):
    """Given the bucket name and group name, return the results folders currently available.  

    """
    bucket = s3_resource.Bucket(b)
    result = s3_client.list_objects(Bucket = b,Prefix = os.path.join(g,"results/"),Delimiter="/")
    folders = [o.get("Prefix") for o in result.get("CommonPrefixes")]

    return folders

def submit_job(b,g, inputname,configname,resultname = None):
    """Submit a job to NeuroCAAS with a given inputname, configname, and optional result timestamp. 

    #TODO: allow bucket bypass
    """
    response = {"submit_filename":None,
                "submit_content":None 
                }

    if resultname is None:
        resultname = time.strftime("%S%M%H%d%b%y") ## second,minute,hour,day,month,year
        time.sleep(1)
        submit_filename = "submit.json"
    else:    
        submit_filename = "{}_submit.json".format(resultname)

    submit_content = {
            "dataname":inputname,
            "configname":configname,
            "timestamp":resultname,
            } 

    key = os.path.join(g,"submissions",submit_filename)
    writeobj = s3_resource.Object(b,key)
    writeobj.put(Body=json.dumps(submit_content,indent = 4).encode("utf-8"))

    response["submit_filename"] = submit_filename
    response["submit_content"] = submit_content
    return response
    
def get_logfiles(bucketname,pathprefix,outputpath):
    """Given a path to a directory, get the logfiles contained in "s3://bucketname/pathprefix/logs/{certificate.txt,DATASET_NAME:{}_STATUS.txt}", and write them to "outputpath/logs/{}".

    :param bucketname: name of the bucket to get logs from.
    :param pathprefix: the path identifying job logs: exclude logs.
    :param outputpath: the path to an existing directory on the local machine. Will create a logs subdirectory if does not exist, and write logs there.
    """
    if not os.path.exists(outputpath):
        print("Output path {} does not exist".format(outputpath))
    filenames = ls_name(bucketname,os.path.join(pathprefix,"logs/"))
    local_logs = os.path.join(outputpath,"logs/")
    if not os.path.exists(local_logs):
        os.mkdir(local_logs)
    for filepath in filenames:
        try:
            filename = os.path.basename(filepath)
            s3_client.download_file(bucketname,filepath,os.path.join(local_logs,filename))
        except IsADirectoryError:
            pass

def get_end(bucketname,pathprefix):
    """Given a path to a directory, look for an "endfile" contained in "s3://bucketname/pathprefix/process_results/end.txt"

    :param bucketname: name of the bucket to get endfile.
    :param pathprefix: the path identifying job: exclude process_results.
    """
    path = os.path.join(pathprefix,"process_results","end.txt")
    endfiles = ls_name(bucketname,path)
    if path in endfiles:
        return True
    else:
        return False

def get_results(bucketname,pathprefix,outputpath):
    """Given a path to a directory, get the result files contained in "s3://bucketname/pathprefix/process_results/", and write them to "outputpath/process_results".

    :param bucketname: name of the bucket to get results from.
    :param pathprefix: the path identifying job process_results: exclude process_results.
    :param outputpath: the path to an existing directory on the local machine. Will create a process_results subdirectory if does not exist, and write results there.
    """
    filenames = ls_name(bucketname,os.path.join(pathprefix,"process_results/"))
    local_results = os.path.join(outputpath,"process_results/")
    if not os.path.exists(local_results):
        os.mkdir(local_results)
    for filepath in filenames:
        try:
            filename = os.path.basename(filepath)
            s3_client.download_file(bucketname,filepath,os.path.join(local_results,filename))
        except IsADirectoryError:
            pass


def poll(bucketname,pathprefix,output):
    """One round of polling a job for logging output. Returns true or false based on the output of get_end.
    :param bucketname: name of the bucket to get logs from.
    :param pathprefix: the path identifying job logs: exclude logs.
    :param outputpath: the path to an existing directory on the local machine. Will create a logs subdirectory if does not exist, and write logs there.
    """
    get_logfiles(bucketname,pathprefix,output)
    return get_end(bucketname,pathprefix)

def setup_polling(bucketname,pathprefix,output,step = 60,timeout = 60*15):
    """Set up polling function

    :param bucketname: name of the bucket to get logs from.
    :param pathprefix: the path identifying job logs: exclude logs.
    :param outputpath: the path to an existing directory on the local machine. Will create a logs subdirectory if does not exist, and write logs there.
    :param step: number of seconds to wait before querying again. Default 60
    :param timeout: timeout for the poll in seconds. Default 15 mins
    :returns: returns an exit code: 0: success, 1: timeout, 2: uncaught exception.
    """
    def ended(response):
        return response == True
    try:
        polling2.poll(
            lambda : poll(bucketname,pathprefix,output),
            check_success = ended,
            step = step,
            timeout = timeout,
            log = logging.INFO)
        get_results(bucketname,pathprefix,output)
        return 0

    except polling2.TimeoutException as te:
        return 1

    except Exception as e:
        print(e)
        return 2


