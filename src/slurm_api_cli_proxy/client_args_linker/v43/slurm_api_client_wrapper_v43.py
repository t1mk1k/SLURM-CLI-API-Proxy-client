from slurm_api_cli_proxy.client_args_linker.slurm_api_client_wrapper import SlurmAPIClientWrapper, ApiClientException, SlurmCommandResponse
from slurm_api_cli_proxy.client_args_linker.constants import slurm_statuses
from openapi_client.models.v0043_openapi_error import V0043OpenapiError
from typing import List
import openapi_client

#sbatch related
from openapi_client.models.v0043_job_submit_req import V0043JobSubmitReq
from openapi_client.models.v0043_openapi_job_submit_response import V0043OpenapiJobSubmitResponse

#squeue related
from openapi_client.models.v0043_openapi_job_info_resp import V0043OpenapiJobInfoResp
from openapi_client.models.v0043_job_info import V0043JobInfo
from openapi_client.models.v0043_job_res import V0043JobRes, V0043JobResNodes
from openapi_client.models.v0043_uint64_no_val_struct import V0043Uint64NoValStruct
from openapi_client.models.v0043_uint16_no_val_struct import V0043Uint16NoValStruct

#scancel related
from openapi_client.models.v0043_kill_jobs_msg import V0043KillJobsMsg
from openapi_client.models.v0043_openapi_kill_jobs_resp import V0043OpenapiKillJobsResp

#scontrol related
from openapi_client.models.v0043_job_desc_msg import V0043JobDescMsg
from openapi_client.models.v0043_openapi_job_post_response import V0043OpenapiJobPostResponse

from openapi_client.rest import ApiException
from openapi_client.exceptions import ServiceException, NotFoundException

import os
import json
import time
import pprint
import slurm_api_cli_proxy.client_args_linker.v43.squeue_format as sqf
from openapi_client.models.v0043_openapi_partition_resp import V0043OpenapiPartitionResp
from openapi_client.models.v0043_partition_info import V0043PartitionInfo
from openapi_client.models.v0043_partition_info_nodes import V0043PartitionInfoNodes
from openapi_client.models.v0043_openapi_partition_resp import V0043OpenapiPartitionResp

class V43SlurmAPIClientWrapper(SlurmAPIClientWrapper):

    # ESLURM_INVALID_JOB_ID, ESLURM_ALREADY_DONE: scancel doesn't report these
    SCANCEL_SILENT_ERRORS = {2017, 2021}

    def sbatch_post_request(self,request:dict,conf:openapi_client.Configuration,slurmrestd_token:str)-> SlurmCommandResponse:     
        #Based on the code snippet included on the documentation generated from the Slurm OpenAPI specification
        configuration = conf
        configuration.api_key['token'] = slurmrestd_token
        with openapi_client.ApiClient(configuration) as api_client:
            # Create an instance of the API class
            
            api_instance = openapi_client.SlurmApi(api_client)

            # Convert the dictionary to a JSON string
            json_req_string = json.dumps(request, indent=2)

            v0043_job_submission_instance = V0043JobSubmitReq.from_json(json_req_string)
            if v0043_job_submission_instance is None:
                raise ValueError(f"Creation of sbatch job submission payload form json returned None when using {json_req_string}")
            
            response = SlurmCommandResponse()

            try:
                # submit the job
                api_response = api_instance.slurm_v0043_post_job_submit(v0043_job_submission_instance)
            except ApiException as ae:
                # Slurm reports rejected submissions (bad partition, account, ...) as HTTP errors
                # with an 'errors' list in the body: show those like sbatch would.
                response.errors.extend(V43SlurmAPIClientWrapper.errors_from_api_exception(ae, "sbatch"))
                return response
            except Exception as e:
                raise ApiClientException(f'Unexpected error while performing a POST request for the sbatch command:{e}') from e

            if api_response.errors:
                for err in api_response.errors:
                    #Based on V0043Error type
                    response.errors.append(f"Error no:{err.error_number}:{err.error}. Source:{err.source}. Description:{err.description}")

            if api_response.job_submit_user_msg:
                # sbatch prints the job_submit plugin's message to stderr
                response.warnings.append(api_response.job_submit_user_msg)

            if not response.errors:
                if api_response.job_id is None:
                    response.errors.append("sbatch: error: the SLURM API did not return a job id")
                else:
                    # Same output as sbatch; tools such as dask-jobqueue parse the job id from it
                    response.output = f"Submitted batch job {api_response.job_id}"
                    response.job_id = api_response.job_id

            return response


    @staticmethod
    def errors_from_api_exception(ae: ApiException, command: str) -> List[str]:
        """Turn the 'errors' list in a failed API response into CLI error lines."""
        messages: List[str] = []
        if ae.body:
            try:
                body = json.loads(ae.body)
                for err in body.get("errors") or []:
                    description = err.get("description") or ""
                    error = err.get("error") or ""
                    messages.append(f"{command}: error: {description}{': ' if description and error else ''}{error}")
            except (ValueError, AttributeError):
                pass
        if not messages:
            messages.append(f"{command}: error: SLURM API request failed (HTTP {ae.status}): {ae.reason}")
        return messages



    def scancel_delete_request(self,request:dict,conf:openapi_client.Configuration,slurmrestd_token:str,quiet:bool=False)-> SlurmCommandResponse:
        # quiet (-Q) is accepted for compatibility: the errors it suppresses in scancel
        # (jobs already gone) are never reported, as in Slurm 25.05.
        configuration = conf
        configuration.api_key['token'] = slurmrestd_token

        with openapi_client.ApiClient(configuration) as api_client:
            api_instance = openapi_client.SlurmApi(api_client)

            kill_jobs_msg = V0043KillJobsMsg.from_dict(request)
            response = SlurmCommandResponse()

            try:
                api_response:V0043OpenapiKillJobsResp = api_instance.slurm_v0043_delete_jobs(kill_jobs_msg)
            except ApiException as ae:
                # A failed request can still list per-job results; use them when present
                try:
                    api_response = V0043OpenapiKillJobsResp.from_json(ae.body) if ae.body else None  # type: ignore[assignment]
                except Exception:
                    api_response = None  # type: ignore[assignment]
                if api_response is None or not api_response.status:
                    response.errors.extend(V43SlurmAPIClientWrapper.errors_from_api_exception(ae, "scancel"))
                    return response
            except Exception as e:
                raise ApiClientException(f'Unexpected error while performing a DELETE request for the scancel command:{e}') from e

            # Only jobs that could not be signalled are listed in 'status'; an empty list means
            # success. Like scancel (Slurm 25.05), jobs that don't exist (anymore) or are
            # already completing are not reported; other failures (e.g. permission denied)
            # are, and the exit code is Slurm's error number modulo 256 (as scancel's).
            for job_status in api_response.status or []:
                err = job_status.error
                if err is None or not (err.code or err.message or err.string):
                    continue
                if err.code in V43SlurmAPIClientWrapper.SCANCEL_SILENT_ERRORS:
                    continue
                job_label = job_status.step_id or (job_status.job_id.number if job_status.job_id else "?")
                message = err.message or err.string or f"error code {err.code}"
                response.errors.append(f"scancel: error: Kill job error on job id {job_label}: {message}")
                if err.code:
                    response.exit_code = err.code % 256 or 1

            for api_err in api_response.errors or []:
                if api_err.description or api_err.error:
                    response.errors.append(f"scancel: error: {api_err.description or api_err.error}")

            return response



    def scontrol_update_request(self,target_job_id:str,request:dict,conf:openapi_client.Configuration,slurmrestd_token:str)-> SlurmCommandResponse:

        configuration = conf
        configuration.api_key['token'] = slurmrestd_token

        with openapi_client.ApiClient(configuration) as api_client:
            # Create an instance of the API class
            
            api_instance = openapi_client.SlurmApi(api_client)

            # Convert the dictionary to a JSON string
            json_req_string = json.dumps(request, indent=2)

            v0043_job_update_payload = V0043JobDescMsg.from_json(json_req_string)

            if v0043_job_update_payload is None:
                raise ValueError(f"Creation of scontrol job submission payload form json returned None when using {json_req_string}")

            response = SlurmCommandResponse()                

            try:
                #submit the update request
                api_response = api_instance.slurm_v0043_post_job(target_job_id, v0043_job_update_payload)
                                
                #Capturing and encapsulating internal errors included as part of the request's response
                if api_response.errors is not None:
                     for err in api_response.errors:
                         #Based on V0043Error type
                         response.errors.append(f"Error no:{err.error_number}:{err.error}. Source:{err.source}. Description:{err.description}")
                
                return response

            #Capturing and encapsulating errors resported as a service exception
            except ServiceException as se:
                if (se.body):
                    exception_body = json.loads(se.body)
                    se_errors:List[dict] = exception_body['errors']
                    for ex_err in se_errors:
                        response.errors.append(f"{ex_err['description']}:{ex_err['error']}")
                    return response
                else:
                    raise ApiClientException(f'Unexpected error while performing an UPDATE request for the scontrol command:{se}') from se                
            
            #Capturing and encapsulating errors related to invalid job_ids, reported by the
            # client as exceptions
            except NotFoundException as ne:
                if (ne.body):
                    exception_body = json.loads(ne.body)
                    ne_errors:List[dict] = exception_body['errors']
                    for ex_err in ne_errors:
                        response.errors.append(f"{ex_err['description']}:{ex_err['error']}")
                    return response
                else:
                    raise ApiClientException(f'Unexpected error while performing an UPDATE request for the scontrol command:{ne}') from ne                
                

            #Any other error would be reported as an unexpected one
            except Exception as e:                
                raise ApiClientException(f'Unexpected error while performing an UPDATE request for the scontrol command:{e}') from e                



    def squeue_get_request(self,cli_arguments:dict,conf:openapi_client.Configuration,slurmrestd_token:str)-> SlurmCommandResponse:    
        #Based on the code snippet included on the documentation generated from the Slurm OpenAPI specification
        configuration = conf
        configuration.api_key['token'] = slurmrestd_token
            
        with openapi_client.ApiClient(configuration) as api_client:
            # Create an instance of the API class
            api_instance = openapi_client.SlurmApi(api_client)
            #update_time = 56 # int | Filter if changed since update_time. Use of this parameter can result in faster replies. (optional)

            try:
                # get list of jobs
                api_response:V0043OpenapiJobInfoResp = api_instance.slurm_v0043_get_jobs()
                
                output = V43SlurmAPIClientWrapper.process_squeue_output(cli_arguments=cli_arguments,api_response=api_response)

                if (api_response.errors is not None):
                    #Transform list of list[V0043Error] to list[str] 
                    errors:list[str] = list(map(lambda err: str(err), api_response.errors))
                else:
                    errors = []

                if (api_response.warnings is not None):
                    #Transform list of list[V0043Warning] to list[str] 
                    warnings:list[str] = list(map(lambda err: str(err), api_response.warnings))
                else:
                    warnings = []

                return SlurmCommandResponse(output=output,errors=errors,warnings=warnings)
        
            except Exception as e:       
                raise ApiClientException(f'Unexpected error while performing a GET request for the squeue command:{e}') from e                


    def sinfo_get_request(self,cli_arguments:dict,conf:openapi_client.Configuration,slurmrestd_token:str)-> SlurmCommandResponse:
        
        configuration = conf
        configuration.api_key['token'] = slurmrestd_token

        with openapi_client.ApiClient(conf) as api_client:    
            api_instance = openapi_client.SlurmApi(api_client)

            try:
                # get all partition info
                api_response:V0043OpenapiPartitionResp = api_instance.slurm_v0043_get_partitions()

                if (api_response.errors is not None):
                    #Transform list of list[V0043Error] to list[str] 
                    errors:list[str] = list(map(lambda err: str(err), api_response.errors))
                else:
                    errors = []

                if (api_response.warnings is not None):
                    #Transform list of list[V0043Warning] to list[str] 
                    warnings:list[str] = list(map(lambda err: str(err), api_response.warnings))
                else:
                    warnings = []

                output = V43SlurmAPIClientWrapper.process_sinfo_output(cli_arguments=cli_arguments,api_response=api_response)
                
                return SlurmCommandResponse(output=output,errors=errors,warnings=warnings)
            
            except Exception as e:
                raise ApiClientException(f'Unexpected error while performing a GET request for the squeue command:{e}') from e 


    @staticmethod
    def process_sinfo_output(cli_arguments:dict, api_response:V0043OpenapiPartitionResp)->str:
        #ref: https://github.com/SLURM-CLI-API-Proxy/SLURM-CLI-API-Proxy-client/blob/main/slurm_api_client/docs/V0043PartitionInfo.md
        partitions:List[V0043PartitionInfo] | None = api_response.partitions

        if partitions != None:
            table = "Partition Name\tTotal Nodes\n"
            table += "-" * 30 + "\n"
            for partition in partitions:
                name = partition.name
                nodes_info: V0043PartitionInfoNodes | None = partition.nodes
                if nodes_info != None:
                    total = nodes_info.total
                else:
                    total = -1
                table += f"{name}\t{total}\n"
            return table
        else:
            return ""



    @staticmethod
    def process_squeue_output(cli_arguments:dict,api_response:V0043OpenapiJobInfoResp)->str:                                                            
        """
        Processes the details returned by the GET request to /jobs so that
        they mimic the output generated by squeue command, according to the
        arguments given (cli_arguments)
        https://slurm.schedmd.com/squeue.html
        
        Args:
            cli_arguments (dict): The arguments given when running the proxy command
            api_response: the response given to a GET request to the /jobs resource            
        Returns:
            str: A formatted string representing the job queue.
        """
        if cli_arguments["--json"] is True:

            api_response_dict:dict = json.loads(api_response.to_json())
            #the "meta" property is not included in the original squeue --json
            api_response_dict.pop("meta")

            return json.dumps(api_response_dict, indent=4)            
        
        else:

            user_filter:str|None = None

            if "--user" in cli_arguments:
                user_filter = cli_arguments["--user"] 
                        
            jobs = api_response.jobs
            if (jobs is None):
                raise ValueError("A list of jobs was expected from the requests to the job resource. A None value was received instead.")

            if "--format" in cli_arguments:
                # custom format from user input
                squeue_format_string = cli_arguments["--format"]
            elif "--long" in cli_arguments:
                squeue_format_string = sqf.long
            elif "--steps" in cli_arguments:
                squeue_format_string = sqf.steps
            else:
                squeue_format_string = sqf.default

            return sqf.format_squeue_output(jobs, squeue_format_string, user_filter)
