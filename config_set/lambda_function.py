import boto3
import botocore
# import jsonschema
import json
import traceback
import zipfile
import os
import hashlib

from botocore.exceptions import ClientError

from extutil import remove_none_attributes, account_context, ExtensionHandler, ext, \
    current_epoch_time_usec_num, component_safe_name, lambda_env, random_id, \
    handle_common_errors, create_zip

eh = ExtensionHandler()

ses = boto3.client('sesv2')

def lambda_handler(event, context):
    try:
        print(f"event = {event}")
        # account_number = account_context(context)['number']
        region = account_context(context)['region']
        eh.capture_event(event)

        prev_state = event.get("prev_state") or {}
        project_code = event.get("project_code")
        repo_id = event.get("repo_id")
        cdef = event.get("component_def")
        cname = event.get("component_name")

        name = cdef.get("name") or component_safe_name(
            project_code, repo_id, cname, max_chars=64
        )

        redirect_domain = cdef.get("redirect_domain")
        tls_policy = cdef.get("tls_policy")
        sending_pool_name = cdef.get("sending_pool_name")

        reputation_metrics_enabled = cdef.get("reputation_metrics_enabled", False)
        #Not doing suppression list for now

        vdm_engagement_metrics = cdef.get("vdm_engagement_metrics", False)
        vdm_optimized_shared_delivery = cdef.get("vdm_optimized_shared_delivery", False)

        tags = cdef.get("tags") or {}
        trust_level = cdef.get("trust_level")
    
        if event.get("pass_back_data"):
            print(f"pass_back_data found")
        elif event.get("op") == "upsert":
            if trust_level == "full":
                eh.add_op("compare_defs")
            else:
                eh.add_op("get_configuration_set")

        elif event.get("op") == "delete":
            eh.add_op("delete_repository", {"create_and_remove": False, "name": name})
            
        compare_defs(event)

        configuration = {
            "ConfigurationSetName": name,
            "TrackingOptions": {
                "CustomRedirectDomain": redirect_domain
            },
            "DeliveryOptions": {
                "TlsPolicy": tls_policy,
                "SendingPoolName": sending_pool_name
            },
            "ReputationOptions": {
                "ReputationMetricsEnabled": reputation_metrics_enabled
            },
            "SendingOptions": {
                "SendingEnabled": True
            },
            "Tags": format_tags(tags),
            "VdmOptions": {
                "DashboardOptions": {
                    "EngagementMetrics": vdm_engagement_metrics,
                },
                "GuardianOptions": {
                    "OptimizedSharedDelivery": vdm_optimized_shared_delivery
                }
            }
        }

        get_configuration_set(prev_state, name, configuration, region, tags)
        create_configuration_set(name, configuration, region)
        put_configuration_set_tracking_options()
        put_configuration_set_sending_options()
        put_configuration_set_reputation_options()
        put_configuration_set_delivery_options()
        add_tags()
        remove_tags()
        delete_configuration_set()
            
        return eh.finish()

    except Exception as e:
        msg = traceback.format_exc()
        print(msg)
        eh.add_log("Unexpected Error", {"error": msg}, is_error=True)
        eh.declare_return(200, 0, error_code=str(e))
        return eh.finish()


@ext(handler=eh, op="compare_defs")
def compare_defs(event):
    old_digest = event.get("prev_state", {}).get("props", {}).get("def_hash")
    new_rendef = event.get("component_def")

    _ = new_rendef.pop("trust_level", None)

    dhash = hashlib.md5()
    dhash.update(json.dumps(new_rendef, sort_keys=True).encode())
    digest = dhash.hexdigest()
    eh.add_props({"def_hash": digest})

    if old_digest == digest:
        eh.add_links(event.get("prev_state", {}).get('links'))
        eh.add_props(event.get("prev_state", {}).get('props'))
        eh.add_log("Full Trust, No Change: Exiting", {"old_hash": old_digest})

    else:
        eh.add_log("Definitions Don't Match, Deploying", {"old": old_digest, "new": digest})
        eh.add_op("get_configuration_set")
        

@ext(handler=eh, op="get_configuration_set")
def get_configuration_set(prev_state, name, configuration, region, tags):

    if prev_state and prev_state.get("props") and prev_state.get("props").get("name"):
        prev_name = prev_state.get("props").get("name")
        if name != prev_name:
            eh.add_op("delete_configuration_set", {
                "create_and_remove": True, 
                "name": prev_name
            })

    try:
        params = {"ConfigurationSetName": name}
        response = ses.get_configuration_set(**params)

        print(f"Get Configuration Set: {response}")

        eh.add_log("Found Configuration Set", response)
        eh.add_props({"name": response['ConfigurationSetName']})
        eh.add_links({"Configuration Set": gen_configuration_set_link(name, region)})

        if response.get("TrackingOptions") != configuration.get("TrackingOptions"):
            eh.add_op("put_configuration_set_tracking_options")
        if response.get("DeliveryOptions") != configuration.get("DeliveryOptions"):
            eh.add_op("put_configuration_set_delivery_options")
        if response.get("ReputationOptions", {}).get("ReputationMetricsEnabled") != configuration.get("ReputationOptions", {}).get("ReputationMetricsEnabled"):
            eh.add_op("put_configuration_set_reputation_options")
        if response.get("SendingOptions") != configuration.get("SendingOptions"):
            eh.add_op("put_configuration_set_sending_options")

        print(f"tags response = {response}")
        current_tags = unformat_tags(response.get("Tags") or [])

        if tags != current_tags:
            remove_tags = [k for k in current_tags.keys() if k not in tags]
            add_tags = {k:v for k,v in tags.items() if k not in current_tags.keys()}
            if remove_tags:
                eh.add_op("remove_tags", remove_tags)
            if add_tags:
                eh.add_op("add_tags", add_tags)

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NotFoundException':
            eh.add_op("create_configuration_set")
        else:
            handle_common_errors(e, eh, "Get Configuration Set", 10)

@ext(handler=eh, op="create_configuration_set")
def create_configuration_set(name, desired_config, region):

    try:
        response = ses.create_configuration_set(**desired_config)
        eh.add_log("Created ECR Repository", response)
        eh.add_props({
            "name": name
        })
        eh.add_links({"Configuration Set": gen_configuration_set_link(name, region)})
    except ClientError as e:
        handle_common_errors(
            e, eh, "Create ECR Repository Failed", 20,
            perm_errors=[
                "LimitExceededException", 
                "RepositoryAlreadyExistsException",
                "InvalidParameterException",
                "InvalidTagParameterException",
                "TooManyTagsException"
            ]
        )


@ext(handler=eh, op="put_configuration_set_tracking_options")
def put_configuration_set_tracking_options():
    name = eh.props.get("name")
    redirect_domain = eh.ops['put_configuration_set_tracking_options']['redirect_domain']

    try:
        response = ses.put_configuration_set_reputation_options(
            ConfigurationSetName=name,
            CustomRedirectDomain= redirect_domain
        )
        eh.add_log("Updated Configuration Set Tracking Options", response)
    except ClientError as e:
        handle_common_errors(
            e, eh, "Update Configuration Set Tracking Options Failed", 44,
            perm_errors=[
                "NotFoundException",
                "BadRequestException"
            ]
        )

@ext(handler=eh, op="put_configuration_set_sending_options")
def put_configuration_set_sending_options():
    name = eh.props.get("name")

    try:
        response = ses.put_configuration_set_sending_options(
            ConfigurationSetName=name,
            SendingEnabled=True
        )
        eh.add_log("Set Configuration Set To Be Active", response)
    except ClientError as e:
        handle_common_errors(
            e, eh, "Settting Configuration Set Active Failed", 46,
            perm_errors=[
                "NotFoundException",
                "BadRequestException"
            ]
        )

@ext(handler=eh, op="put_configuration_set_reputation_options")
def put_configuration_set_reputation_options():
    name = eh.props.get("name")
    reputation_enabled = eh.ops['put_configuration_set_reputation_options']['reputation_enabled']

    try:
        response = ses.put_configuration_set_reputation_options(
            ConfigurationSetName=name,
            ReputationMetricsEnabled= reputation_enabled
        )
        eh.add_log("Updated Configuration Set Reputation Options", response)
    except ClientError as e:
        handle_common_errors(
            e, eh, "Update Configuration Set Reputation Options Failed", 48,
            perm_errors=[
                "NotFoundException",
                "BadRequestException"
            ]
        )

@ext(handler=eh, op="put_configuration_set_delivery_options")
def put_configuration_set_delivery_options():
    name = eh.props.get("name")
    tls_policy = eh.ops['put_configuration_set_delivery_options']['tls_policy']
    pool_name = eh.ops['put_configuration_set_delivery_options']['sending_pool_name']

    try:
        response = ses.put_configuration_set_delivery_options(
            ConfigurationSetName=name,
            TlsPolicy= tls_policy,
            SendingPoolName= pool_name
        )
        eh.add_log("Updated Configuration Set Delivery Options", response)
    except ClientError as e:
        handle_common_errors(
            e, eh, "Update Configuration Set Delivery Options Failed", 52,
            perm_errors=[
                "NotFoundException",
                "BadRequestException"
            ]
        )

@ext(handler=eh, op="delete_configuration_set")
def delete_configuration_set():
    config_set_name = eh.ops['delete_configuration_set'].get("name")
    car = eh.ops['delete_configuration_set'].get("create_and_remove")

    try:
        params = {
            "ConfigurationSetName": config_set_name
        }

        _ = ses.delete_repository(**params)
        eh.add_log("Deleted Configuration Set", {"name": config_set_name})

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "NotFoundException":
            eh.add_log("Old Set Does Not Exist", {"name": config_set_name})
        else:
            handle_common_errors(e, eh, "Delete Config Set Failed", 80 if car else 10)

@ext(handler=eh, op="add_tags")
def add_tags():
    tags = format_tags(eh.ops['add_tags'])
    arn = eh.props['arn']

    try:
        ses.tag_resource(
            ResourceArn=arn,
            Tags=tags
        )
        eh.add_log("Tags Added", {"tags": tags})

    except ClientError as e:
        handle_common_errors(e, eh, "Add Tags Failed", 50, ['InvalidParameterValueException'])
        
@ext(handler=eh, op="remove_tags")
def remove_tags():
    arn = eh.props['arn']

    try:
        ses.untag_resource(
            ResourceArn=arn,
            TagKeys=eh.ops['remove_tags']
        )
        eh.add_log("Tags Removed", {"tags": eh.ops['remove_tags']})

    except botocore.exceptions.ClientError as e:
        handle_common_errors(e, eh, "Remove Tags Failed", 65, ['InvalidParameterValueException'])

def format_tags(tags_dict):
    return [{"Key": k, "Value": v} for k,v in tags_dict]

def unformat_tags(tags_list):
    return {t["Key"]: t["Value"] for t in tags_list}

def gen_configuration_set_link(name, region):
    return f"https://{region}.console.aws.amazon.com/ses/home?region={region}#/configuration-sets/{name}"



