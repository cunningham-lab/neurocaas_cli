## contains CLI commands. 
import click 
import os 
import json
from neurocaas_cli import analyze as analyze_mod 


## configuration file settings:
configname = ".neurocaas_cli_config.json"
configpath = os.path.join(os.path.expanduser("~"),configname)

## main functions
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
    """Functions to data. 

    """

@analyze.command(help = "upload data to user data location in NeuroCAAS")
@click.option("-d","--datapath",help = "path(s) to local file(s) you will upload as data", multiple = True)
@click.pass_obj
def upload_data(ctx,datapath):
    """Upload a file located at "datapath" to the user's S3 location. 

    """
    for datap in datapath: 
        response = analyze_mod.upload_data(ctx.obj["bucketname"],ctx.obj["groupprefix"],datap)
    click.echo(response)    

@analyze.command(help = "upload config file to user config location in NeuroCAAS")
@click.option("-c","--configpath",help = "path(s) to local file(s) you will upload as config", multiple = True)
@click.pass_obj
def upload_config(ctx,configpath):    
    """

    """
    for configp in configpath: 
        response = analyze_mod.upload_config(configp)
    click.echo(response)    

        
@analyze.command(help = "list data and config files currently uploaded to NeuroCAAS")
@click.pass_obj
def list_inputs(ctx):    
    """

    """

    datafiles, configfiles = analyze_mod.list_inputs(ctx["bucketname"],ctx["groupprefix"])
    click.echo("############### Data files: #################")
    click.echo("\n".join(datafiles))
    click.echo("############### Config files: #################")
    click.echo("\n".join(configfiles))
    
@analyze.command(help = "submit a job to NeuroCAAS using data and config that is on local computer or in NeuroCAAS storage.")    
@click.option("-d","--datapath",help = "path(s) to uploaded data for analysis assuming group name as prefix",multiple = True)
@click.option("-c","--configpath",help = "path to uploaded config for analysis assuming group name as prefix")
@click.option("-r","--resulttag",help = "timestamp to associate with job (optional)",default = None)
@click.pass_obj
def submit_job(ctx,datapath,configpath,resulttag):    
    """

    """
    response = analyze_mod.submit_job(ctx["bucketname"],ctx["groupprefix"],datapath,configpath,resulttag)
    click.echo(response)

@analyze.command(help = "list existing results for different analyses on NeuroCAAS")
@click.pass_obj
def list_results(ctx,):
    """

    """
    response = analyze_mod.list_results(ctx["bucketname"],ctx["groupprefix"])
    click.echo("\n".join(response))

@analyze.command(help = "poll an ongoing analysis for logs and results.")
@click.option("-l","--localpath",help = "local directory to which we should write results.")
@click.option("-rt","--resulttag",help = "timestamp associated with job to poll. One of resulttag or resultpath must be given.",default = None)
@click.option("-rp","--resultpath",help = "full folder name associated with job to poll. One of resulttag or resultpath must be given.",default = None)
@click.option("-i","--interval",help = "interval between polling in seconds. (default 60 seconds)",default = 60 )
@click.option("-t","--timeout",help = "timeout for the poll in seconds. (default 15 mins)", default = 60*15)
@click.pass_obj
def setup_polling(ctx,localpath,resulttag,resultpath,interval,timeout):
    """

    """
    ## choose between timestamp or path output. 
    assert resulttag is not None or resultpath is not None
    assert not all(r is None for r in [resulttag,resultpath])
    if resulttag is not None:
        resultpath = "job__{}_{}".format(ctx["bucketname"],resulttag)
    else:    
        pass
    outcome = analyze_mod.setup_polling(ctx["bucketname"],os.path.join(ctx["groupprefix"],"results",resultpath),localpath,interval,timeout)
    outcome_codes = {0:"Success. See {} for results".format(localpath),
                     1:"Timeout. Run polling again to keep monitoring for output.",
                     2:"Unhandled Exception. See message above. "}
    click.echo(outcome_codes[outcome])


@analyze.command(help = "simultaneously submit a job and poll for results")
@click.option("-d","--datapath",help = "path(s) to uploaded data for analysis assuming group name as prefix",multiple = True)
@click.option("-c","--configpath",help = "path to uploaded config for analysis assuming group name as prefix")
@click.option("-l","--localpath",help = "local directory to which we should write results.")
@click.option("-r","--resulttag",help = "timestamp to associate with job (optional)",default = None)
@click.option("-i","--interval",help = "interval between polling in seconds. (default 60 seconds)",default = 60 )
@click.option("-t","--timeout",help = "timeout for the poll in seconds. (default 15 mins)", default = 60*15)
@click.pass_obj
def submit_and_poll(ctx,datapath,configpath,localpath,resulttag,interval,timeout):    
    """

    """
    submit_response = analyze_mod.submit_job(ctx["bucketname"],ctx["groupprefix"],datapath,configpath,resulttag)
    click.echo("Job submitted. Starting polling.")
    resultpath = "job__{}_{}".format(ctx["bucketname"],submit_response["submit_content"]["timestamp"])
    outcome = analyze_mod.setup_polling(ctx["bucketname"],os.path.join(ctx["groupprefix"],"results",resultpath),localpath,interval,timeout)
    outcome_codes = {0:"Success. See {} for results".format(localpath),
                     1:"Timeout. Run polling again to keep monitoring for output.",
                     2:"Unhandled Exception. See message above. "}
    click.echo(outcome_codes[outcome])

