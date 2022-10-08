## test suite for analyze.py 
import pytest
import localstack_client.session
import logging
from botocore.exceptions import ClientError

test_result_mats = os.path.join(loc,"test_mats","test_aws_resource","test_analyze")

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
    monkeypatch.setattr(monitor, "s3_client", session.client("s3")) ## TODO I don't think these are scoped correctly w/o a context manager.
    monkeypatch.setattr(monitor, "s3_resource", session.resource("s3"))

    ## Create bucket if not created:
    try:
        buckets = s3_client.list_buckets()["Buckets"]
        bucketnames = [b["Name"] for b in buckets]
        assert result_bucket_name in bucketnames
        yield result_bucket_name
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
        yield result_bucket_name    
        ## remove from inputs, configs, and submissions after each test. 
        clear_subdirectory(result_bucket_name,"user1/inputs")
        clear_subdirectory(result_bucket_name,"user1/configs")
        clear_subdirectory(result_bucket_name,"user1/submissions")

def clear_subdirectory():
    """Clears out a subdirectory of an S3 bucket for easier testing.

    """

def test_upload_data():
    """Test that multiple upload files are correctly uploaded. 

    """

def test_upload_config():    
    """Test that multiple config files are correctly uploaded. 

    """

def test_list_inputs():    
    """test that all component inputs are listed

    """

def test_list_results():
    """test that all results are listed

    """

def test_submit_job():
    """test that submit file is correctly uploaded. 

    """

def test_poll_results():    
    """Test that polling results in local files as expected. 

    """

def test_submit_and_poll():    
    """Test that correct submit file is created, and corresponding results directories are polled. 

    """

