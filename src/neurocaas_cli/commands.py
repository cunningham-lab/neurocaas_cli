## contains CLI commands. 
import click 
import os 
import json


## configuration file settings:
configname = ".neurocaas_cli_config.json"
configpath = os.path.join(os.path.expanduser("~"),configname)

@click.group(help = "base command for the CLI")
@click.pass_context
def cli(ctx): 
    ## assign registration info to the context if exists. 
    try:
        with open(configpath,"r") as f:
            config_dict = json.load(f)
        ctx.obj = config_dict    
    ## if not exists, assert that must be initialized.  
    except (FileNotFoundError,click.ClickException,KeyError):    
        if ctx.invoked_subcommand =="init":
            return ## move on and run configure. 
        else:
            raise click.ClickException("Configuration file not found. Run `neurocaas-cli init` to initialize the cli.")
        
@cli.command(help = "Initialize CLI with user data.")
@click.option("--bucketname",help = "Name of S3 bucket to interact with.")
@click.option("--groupprefix",help = "Prefix of S3 bucket identifying this user")
def init(bucketname,groupprefix):
    """Configure CLI with the bucket and username used to identify a certain S3 bucket to work with. 
    """
    obj = {}
    obj["bucketname"] = bucketname
    obj["groupprefix"] = groupprefix
    print("writing/updating config file")
    with open(configpath,"w") as f:
        json.dump(obj,f,indent = 4)

@cli.group(help = "Analyze data with CLI")
@click.pass_context
def analyze(ctx):
    """Functions to analyze data. 

    """

@analyze.command(help = "upload data to user data location in NeuroCAAS")
@analyze.option("-d","--datapath",help = "path(s) to local file(s) you will upload as data", multiple = True)
@click.pass_context
def upload_data():
    """

    """

@analyze.command(help = "upload config file to user config location in NeuroCAAS")
@analyze.option("-c","--configpath",help = "path(s) to local file(s) you will upload as config", multiple = True)
@click.pass_context
def upload_config():    
    """

    """

@analyze.command(help = "list data and config files currently uploaded to NeuroCAAS")
@click.pass_context
def list_inputs():    
    """

    """
    
@analyze.command(help = "submit a job to NeuroCAAS using data and config that is on local computer or in NeuroCAAS storage.")    
@analyze.option("-d","--datapath",help = "path(s) to uploaded data for analysis assuming group name as prefix",multiple = True)
@analyze.option("-c","--configpath",help = "path to uploaded config for analysis assuming group name as prefix")
@analyze.option("-r","--resulttag",help = "timestamp to associate with job (optional)",default = None)
@click.pass_context
def submit_job():    
    """

    """

@analyze.command(help = "list existing results for different analyses on NeuroCAAS")
@click.pass_context
def list_results():
    """

    """

@analyze.command(help = "poll an ongoing analysis for logs and results.")
@analyze.option("-l","--localpath",help = "local directory to which we should write results.")
@analyze.option("-r","--resulttag",help = "timestamp associated with job to poll")
@click.pass_context
def poll_results():
    """

    """

@analyze.command(help = "simultaneously submit a job and poll for results")
@analyze.option("-d","--datapath",help = "path(s) to uploaded data for analysis assuming group name as prefix",multiple = True)
@analyze.option("-c","--configpath",help = "path to uploaded config for analysis assuming group name as prefix")
@analyze.option("-r","--resulttag",help = "timestamp to associate with job (optional)",default = None)
@analyze.option("-l","--localpath",help = "local directory to which we should write results.")
@click.pass_context
def submit_and_poll():    
    """

    """

