## test suite for analyze.py 
import os
import pytest
import localstack_client.session
import logging
from botocore.exceptions import ClientError
from neurocaas_cli import analyze,Interface_S3

loc = os.path.abspath(os.path.dirname(__file__))
test_result_mats = os.path.join(loc,"test_mats","test_aws_resource","test_analyze")
test_upload_mats = os.path.join(loc,"test_mats","test_local_mats")
result_bucket_name = "cli-analyze-bucket"

def get_paths(rootpath):
    """Gets paths to all files relative to a given top level path. 

    """
    walkgen = os.walk(rootpath)
    paths = []
    dirpaths = []
    for p,dirs,files in walkgen:
        relpath = os.path.relpath(p,rootpath)
        if len(files) > 0 or len(dirs) > 0:
            for f in files:
                localfile = os.path.join(relpath,f)
                paths.append(localfile)
            ## We should upload the directories explicitly, as they will be treated in s3 like their own objects and we perform checks on them.    
            for d in dirs:
                localdir = os.path.join(relpath,d,"")
                if localdir == "./logs/":
                    dirpaths.append("logs/")
                else:
                    dirpaths.append(localdir)
    return paths,dirpaths            

@pytest.fixture
def setup_analysis_bucket(monkeypatch):
    """Sets up the module to use localstack, and creates a bucket in localstack called test-analyze-cli with the following directory structure:
    /
    |-user1
      |-inputs
      |-configs
      |-submissions
      |-results
        |-completed_job
          |-logs
            |-certificate.txt
            |-DATASTATUS.json
            |-logfile.txt
          |-process_results  
            |-end.txt
        |-uncompleted_job
          |-logs
            |-certificate.txt
            |-DATASTATUS.json
            |-logfile.txt
          |-process_results  
        ...
    This is the minimal working example for testing a monitoring function. This assumes that we will not be mutating the state of bucket logs. 
    """
    ## Start localstack and patch AWS clients:
    session = localstack_client.session.Session()
    s3_client = session.client("s3")
    s3_resource = session.resource("s3")
    monkeypatch.setattr(analyze, "s3_client", session.client("s3")) ## TODO I don't think these are scoped correctly w/o a context manager.
    monkeypatch.setattr(analyze, "s3_resource", session.resource("s3"))

    ## Create bucket if not created:
    try:
        buckets = s3_client.list_buckets()["Buckets"]
        bucketnames = [b["Name"] for b in buckets]
        assert result_bucket_name in bucketnames
        yield result_bucket_name,"user1"
    except AssertionError:    
        s3_client.create_bucket(Bucket =result_bucket_name)

        ## Get paths:
        log_paths,dirpaths = get_paths(test_result_mats) 
        try:
            for f in log_paths:
                s3_client.upload_file(os.path.join(test_result_mats,f),result_bucket_name,Key = f)
            for dirpath in dirpaths:
                s3dir = s3_resource.Object(result_bucket_name,dirpath)   
                s3dir.put()
        except ClientError as e:        
            logging.error(e)
            raise
        yield result_bucket_name,"user1"    
    ## remove from inputs, configs, and submissions after each test. 
    clear_subdirectory(result_bucket_name,"user1/inputs/")
    clear_subdirectory(result_bucket_name,"user1/configs/")
    clear_subdirectory(result_bucket_name,"user1/submissions/")

def clear_subdirectory(bucketname,prefix):
    """Clears out a subdirectory of an S3 bucket for easier testing.

    """
    session = localstack_client.session.Session()
    s3_client = session.client("s3")
    s3_resource = session.resource("s3")
    bucket = s3_resource.Bucket(bucketname)
    data = [objname.key for objname in bucket.objects.filter(Prefix = prefix)] 
    for d in data:
        if d == prefix:
            continue
        else:
            s3_client.delete_object(Bucket = bucketname,Key = d)

def test_upload_data(monkeypatch,setup_analysis_bucket):
    """Test that multiple upload files are correctly uploaded. 

    """
    session = localstack_client.session.Session()
    s3_client = session.client("s3")
    s3_resource = session.resource("s3")
    monkeypatch.setattr(Interface_S3, "s3_client", session.client("s3")) 
    monkeypatch.setattr(Interface_S3, "s3", session.resource("s3"))

    b,p = setup_analysis_bucket
    bucket = s3_resource.Bucket(b)
    filename = [os.path.join(test_upload_mats,f) for f in ["inputs/datafile.json","inputs/datafile_2.json"]]
    for f in filename:
        response = analyze.upload_data(b,p,f)
        assert response["uploaded_file_path"] == f    
        datafiles  = [os.path.basename(objname.key) for objname in bucket.objects.filter(Prefix = os.path.join(p,"inputs/"))] 
        assert os.path.basename(f) in datafiles

def test_upload_config(monkeypatch,setup_analysis_bucket):    
    """Test that multiple config files are correctly uploaded. 

    """
    session = localstack_client.session.Session()
    s3_client = session.client("s3")
    s3_resource = session.resource("s3")
    monkeypatch.setattr(Interface_S3, "s3_client", session.client("s3")) 
    monkeypatch.setattr(Interface_S3, "s3", session.resource("s3"))

    b,p = setup_analysis_bucket
    bucket = s3_resource.Bucket(b)
    filename = [os.path.join(test_upload_mats,f) for f in ["configs/config.yaml","configs/config_2.yaml"]]
    for f in filename:
        response = analyze.upload_config(b,p,f)
        assert response["uploaded_file_path"] == f    
        configfiles  = [os.path.basename(objname.key) for objname in bucket.objects.filter(Prefix = os.path.join(p,"configs/"))] 
        assert os.path.basename(f) in configfiles

def test_list_inputs(setup_analysis_bucket):    
    """test that all component inputs are listed

    """
    b,p = setup_analysis_bucket
    data,configs = analyze.list_inputs(b,p)
    print(data,configs)
    assert len(data) == 0
    assert len(configs) == 0
    

def test_list_results(setup_analysis_bucket):
    """test that all results are listed

    """
    b,p = setup_analysis_bucket
    results = analyze.list_results(b,p)
    assert len(results) == 2
    for r in results:
        assert os.path.basename(os.path.abspath(r)) in ["completed_job","uncompleted_job"]

def test_submit_job(monkeypatch,setup_analysis_bucket):
    """test that submit file is correctly uploaded. 

    """
    session = localstack_client.session.Session()
    s3_client = session.client("s3")
    s3_resource = session.resource("s3")
    monkeypatch.setattr(Interface_S3, "s3_client", session.client("s3")) 
    monkeypatch.setattr(Interface_S3, "s3", session.resource("s3"))

    b,p = setup_analysis_bucket
    bucket = s3_resource.Bucket(b)
    inputname = "dataset.json"
    configname = "config.yaml"
    resultname = "timestamp"

    cust_response = analyze.submit_job(b,p,inputname,configname,resultname)
    assert cust_response["submit_filename"] == "{}_submit.json".format(resultname)    
    assert cust_response["submit_content"]["timestamp"] == resultname

    response = analyze.submit_job(b,p,inputname,configname)
    assert response["submit_filename"] == "submit.json".format(resultname)    
    assert response["submit_content"]["timestamp"] is not None 

    submitfiles  = [os.path.basename(objname.key) for objname in bucket.objects.filter(Prefix = os.path.join(p,"submissions/"))] 
    for r in [cust_response,response]:
        assert r["submit_filename"] in submitfiles

def test_get_logfiles(setup_analysis_bucket,tmp_path):
    bucket_name,path_prefix = setup_analysis_bucket
    analyze.get_logfiles(bucket_name,"user1/results/completed_job",str(tmp_path))
    contents = os.listdir(os.path.join(tmp_path,"logs/"))

    for c in ["certificate.txt","DATASTATUS.json","logfile.txt"]:
        assert c in [os.path.basename(ci) for ci in contents] 
   
@pytest.mark.parametrize("path,out",[("user1/results/completed_job",True),("user1/results/uncompleted_job",False)])
def test_get_end(setup_analysis_bucket,path,out):   
    bucket_name,path_prefix = setup_analysis_bucket
    assert out == analyze.get_end(bucket_name,path)

@pytest.mark.parametrize("path,out,local",[("user1/results/completed_job",True,"sub1"),("user1/results/uncompleted_job",False,"sub2")])
def test_poll(setup_analysis_bucket,tmp_path,path,out,local):
    bucket_name,path_prefix = setup_analysis_bucket
    sub_write = os.path.join(tmp_path,local)
    os.mkdir(sub_write)
    assert out == analyze.poll(bucket_name,path,sub_write)
    assert "certificate.txt" in os.listdir(os.path.join(sub_write,"logs")) 

@pytest.mark.parametrize("path,out,local",[("user1/results/completed_job",True,"sub1"),("user1/results/uncompleted_job",False,"sub2")])
def test_setup_polling(setup_analysis_bucket,tmp_path,path,out,local):
    """Tests that exit codes are correct, and polling returns results after successfully completing. 

    """
    bucket_name,path_prefix = setup_analysis_bucket
    sub_write = os.path.join(tmp_path,local)
    os.mkdir(sub_write)
    assert analyze.setup_polling(bucket_name,path,sub_write,1,5) == 1-out
    if out == True:
        assert "end.txt" in os.listdir(os.path.join(sub_write,"process_results"))


