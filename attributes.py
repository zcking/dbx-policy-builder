import streamlit as st
from databricks.sdk.environments import Cloud
from typing import Callable, Any
from collections import OrderedDict

# ===== Attribute Logic Helpers =====

def set_toggle_options(attribute_name: str):
    at = st.session_state.get(f'{attribute_name}__attribute_type', 'fixed')
    if at in ('fixed', 'forbidden'):
        # Fixed and forbidden attributes don't make sense to set a default value or make optional
        st.session_state['toggle_options'] = ['Hide from UI']
    elif at in ('allowlist', 'blocklist'):
        st.session_state['toggle_options'] = ['Set Default Value', 'Make Optional']
    elif at in ('regex', 'range', 'unlimited'):
        st.session_state['toggle_options'] = ['Set Default Value', 'Make Optional']
    else:
        st.session_state['toggle_options'] = []

def set_attribute_description(description: str):
    st.session_state['attribute_description'] = description

def _attribute_type(attribute_name: str, default_value_input: Callable[[], Any] = None,
                    range: bool = False, allow_list: bool = True, 
                    block_list: bool = True, regex: bool = True, 
                    unlimited: bool = True) -> str:
    # Single select for the `type` of policy attribute to set.
    options = [
        'fixed',
        'forbidden',
        'regex' if regex else None,
        'range' if range else None,
        'allowlist' if allow_list else None,
        'blocklist' if block_list else None,
        'unlimited' if unlimited else None,
    ]

    def _handle_attribute_type_change():
        st.session_state['inputs'].clear()
        set_toggle_options(attribute_name)

    if st.session_state.get('attribute_description'):
        st.write('Description')
        st.write(st.session_state['attribute_description'])

    if st.session_state['cloud'] == Cloud.AWS:
        policy_types_docs_url = "https://docs.databricks.com/aws/en/admin/clusters/policy-definition#supported-policy-types"
    elif st.session_state['cloud'] == Cloud.GCP:
        policy_types_docs_url = "https://docs.databricks.com/gcp/en/admin/clusters/policy-definition#supported-policy-types"
    elif st.session_state['cloud'] == Cloud.AZURE:
        policy_types_docs_url = "https://learn.microsoft.com/en-us/azure/databricks/admin/clusters/policy-definition#supported-policy-types"

    attr_types = [option for option in options if option is not None]
    attribute_type = st.radio(
        'Type',
        options=attr_types,
        key=f'{attribute_name}__attribute_type',
        on_change=_handle_attribute_type_change,
        horizontal=True,
        help=f"""Select the policy type for this attribute. See 
        [Policy Types]({policy_types_docs_url})
        for more information.""",
    )
    st.session_state['inputs']['type'] = attribute_type
    set_toggle_options(attribute_name)

    if default_value_input:
        toggle_options = st.session_state['toggle_options']
        toggles = st.pills(
            label='Features',
            key=f'{attribute_name}__toggle_group',
            options=toggle_options,
            selection_mode='multi',
            help='Toggle optional features for the attribute',
        )

        # Set Default Value
        if 'Set Default Value' in toggles:
            default_value = default_value_input()
            st.session_state['inputs']['defaultValue'] = default_value
        elif 'defaultValue' in st.session_state['inputs']:
            st.session_state['inputs'].pop('defaultValue')

        # Optional
        if 'Make Optional' in toggles:
            st.session_state['inputs']['isOptional'] = True
        elif 'isOptional' in st.session_state['inputs']:
            st.session_state['inputs'].pop('isOptional')

        # Hide from UI
        if 'Hide from UI' in toggles:
            st.session_state['inputs']['hidden'] = True
        elif 'hidden' in st.session_state['inputs']:
            st.session_state['inputs'].pop('hidden')
    return attribute_type

def gen_number_attribute_ui(attribute_name: str, _min_value: int, _max_value: int, _default_value: int):
    def _default_value_input():
        return st.number_input(
            'Default Value',
            min_value=_min_value,
            max_value=_max_value,
            value=_default_value,
            key=f'{attribute_name}__default_value_input',
        )

    at = _attribute_type(
        attribute_name,
        regex=False,
        range=True,
        allow_list=False,
        block_list=False,
        default_value_input=_default_value_input,
    )
    if at == 'range':
        col1, col2 = st.columns(2)
        with col1:
            min_value = st.number_input('Min Value', min_value=_min_value, max_value=_max_value, value=_default_value, key=f'{attribute_name}__min_value')
        with col2:
            max_value = st.number_input('Max Value', min_value=min_value, max_value=_max_value, key=f'{attribute_name}__max_value')
        st.session_state['inputs']['minValue'] = min_value
        st.session_state['inputs']['maxValue'] = max_value
    elif at == 'fixed':
        st.session_state['inputs']['value'] = st.number_input(
            'Fixed Value',
            min_value=_min_value,
            max_value=_max_value,
            value=_default_value,
            key=f'{attribute_name}__fixed_value_input',
        )

def gen_string_attribute_ui(attribute_name: str, 
                            _options: list[str] | None = None,
                            _placeholder: str = 'Enter a value',
                            _format_func: Callable[[Any], Any] | None = str):
    def _default_handler():
        if _options:
            return st.selectbox('Default Value', options=_options, index=None, format_func=_format_func)
        else:
            return st.text_input('Default Value', placeholder=_placeholder)

    at = _attribute_type(
        attribute_name,
        range=False,
        allow_list=True,
        block_list=True,
        regex=True,
        default_value_input=_default_handler,
    )
    if at in ('allowlist', 'blocklist'):
        if _options:
            values = st.multiselect(
                'Values',
                options=_options,
                key=f'{attribute_name}__values',
                format_func=_format_func,
            )
            st.session_state['inputs']['values'] = values
        else:
            values = st.data_editor(
                data=[
                    {"value": _placeholder},
                ],
                num_rows='dynamic',
                key=f'{attribute_name}__values',
            )
            st.session_state['inputs']['values'] = [row['value'] for row in values]
    elif at == 'fixed':
        if _options:
            fixed_value = st.selectbox('Fixed Value', options=_options, index=None, format_func=_format_func)
        else:
            fixed_value = st.text_input('Fixed Value', placeholder=_placeholder)
        st.session_state['inputs']['value'] = fixed_value
    elif at == 'regex':
        regex_input = st.text_input('Regex Pattern', placeholder='^...$')
        st.session_state['inputs']['pattern'] = regex_input

def gen_boolean_attribute_ui(attribute_name: str, default_value: bool = False):
    at = _attribute_type(
        attribute_name,
        range=False,
        allow_list=False,
        block_list=False,
        regex=False,
        default_value_input=lambda: st.checkbox('Default Value', value=default_value),
    )
    if at == 'fixed':
        st.session_state['inputs']['value'] = st.checkbox('Enabled', value=default_value)

def gen_array_string_attribute_ui(attribute_name: str):
    if st.checkbox('Apply policy to all values'):
        st.session_state['override_attribute_name_select'] = attribute_name
        gen_string_attribute_ui(
            attribute_name=attribute_name,
            _placeholder='/dbfs/...',
        )
    else:
        index = st.number_input('Apply policy to value at index {X}', min_value=0, max_value=100000, value=0)
        if index >= 0:
            indexed_attribute_name = attribute_name.replace('*', str(index))
            st.session_state['override_attribute_name_select'] = indexed_attribute_name
            gen_string_attribute_ui(
                attribute_name=indexed_attribute_name,
                _placeholder='/dbfs/...',
            )

# ===== Individual Attribute UI Functions =====

def spark_version():
    # Set up the default value input logic
    show_these_last = st.session_state['spark_versions']
    special_options = [
        'auto:latest-lts',
        'auto:latest',
        'auto:latest-ml',
        'auto:latest-lts-ml',
        'auto:prev-major',
        'auto:prev-major-ml',
        'auto:prev-lts',
        'auto:prev-lts-ml',
    ]
    spark_versions = OrderedDict()
    for o in special_options:
        spark_versions[o] = f"** {o}"
    spark_versions.update(show_these_last)

    def _default_value_input():
        return st.selectbox(
            'Default Value',
            options=list(spark_versions),
            key="spark_version__default_value_select",
            index=None,
            format_func=lambda x: spark_versions[x],
        )

    _attribute_type('spark_version', _default_value_input)

    # If the selection type is `allowlist` or `blocklist`, we need to allow the user to add multiple values
    if st.session_state['inputs']['type'] in ('allowlist', 'blocklist'):
        st.subheader(st.session_state['inputs']['type'].title() + ' Values')
        # TODO: replace data_editor with a better UI for adding multiple values
        values = st.data_editor(
            data=[
                {"spark_version": special_options[0]},
            ],
            num_rows='dynamic',
            key='spark_version__values',
            column_config={
                'spark_version': st.column_config.TextColumn(required=True),                    
            },
            hide_index=True,
            width=200,
        )
        st.session_state['inputs']['values'] = [row['spark_version'] for row in values]
    elif st.session_state['inputs']['type'] == 'regex':
        st.subheader('Regex')
        regex_input = st.text_input('Regex Pattern', placeholder='^...$')
        st.session_state['inputs']['pattern'] = regex_input
    elif st.session_state['inputs']['type'] == 'fixed':
        fixed_value = st.selectbox(
            'Fixed Value',
            options=list(spark_versions),
            key="spark_version__fixed_value_select",
            index=None,
            format_func=lambda x: spark_versions[x],
        )
        st.session_state['inputs']['value'] = fixed_value

def autoscale_min_workers():
    gen_number_attribute_ui(
        attribute_name='autoscale.min_workers',
        _min_value=0,
        _max_value=100000,
        _default_value=1,
    )

def autoscale_max_workers():
    gen_number_attribute_ui(
        attribute_name='autoscale.max_workers',
        _min_value=0,
        _max_value=100000,
        _default_value=1,
    )

def autotermination_minutes():
    set_attribute_description('A value of 0 represents no auto termination. When hidden, removes the auto termination checkbox and value input from the UI.')
    gen_number_attribute_ui(
        attribute_name='autotermination_minutes',
        _min_value=10,
        _max_value=43200,
        _default_value=60,
    )

def aws_attributes_availability():
    set_attribute_description('Controls AWS availability (SPOT, ON_DEMAND, or SPOT_WITH_FALLBACK)')
    options = ['ON_DEMAND', 'SPOT', 'SPOT_WITH_FALLBACK']
    gen_string_attribute_ui(
        attribute_name='aws_attributes.availability',
        _options=options,
        _placeholder='ON_DEMAND',
    )

def aws_attributes_ebs_volume_count():
    set_attribute_description('The number of AWS EBS volumes.')
    gen_number_attribute_ui(
        attribute_name='aws_attributes.ebs_volume_count',
        _min_value=1,
        _max_value=28,
        _default_value=1,
    )

def aws_attributes_ebs_volume_size():
    set_attribute_description('The size (in GiB) of AWS EBS volumes.')
    gen_number_attribute_ui(
        attribute_name='aws_attributes.ebs_volume_size',
        _min_value=1,
        _max_value=16384,
        _default_value=100,
    )

def aws_attributes_ebs_volume_type():
    set_attribute_description('The type of AWS EBS volumes.')
    options = [
        'GENERAL_PURPOSE_SSD',
        'THROUGHPUT_OPTIMIZED_HDD',
    ]
    gen_string_attribute_ui(
        attribute_name='aws_attributes.ebs_volume_type',
        _options=options,
        _placeholder=options[0] if options else 'GENERAL_PURPOSE_SSD',
    )

def aws_attributes_first_on_demand():
    set_attribute_description('Controls the number of nodes to put on on-demand instances.')
    gen_number_attribute_ui(
        attribute_name='aws_attributes.first_on_demand',
        _min_value=0,
        _max_value=100000,
        _default_value=1,
    )

def aws_attributes_instance_profile_arn():
    set_attribute_description('The ARN of the instance profile to use for the cluster.')
    options = st.session_state['instance_profiles']
    gen_string_attribute_ui(
        attribute_name='aws_attributes.instance_profile_arn',
        _options=options,
        _placeholder='arn:aws:iam::123456789012:instance-profile/my-instance-profile',
    )

def aws_attributes_spot_bid_price_percent():
    set_attribute_description('Controls the maximum price for AWS spot instances.')
    gen_number_attribute_ui(
        attribute_name='aws_attributes.spot_bid_price_percent',
        _min_value=1,
        _max_value=100,
        _default_value=100,
    )

def aws_attributes_zone_id():
    set_attribute_description('The AWS zone ID to use for the cluster.')
    options = st.session_state['zones']
    gen_string_attribute_ui(
        attribute_name='aws_attributes.zone_id',
        _options=options,
        _placeholder=options[0] if options else 'us-east-1a',
    )

def cluster_log_conf_path():
    set_attribute_description('The destination URL of the log files. This can also be a Volume.')
    gen_string_attribute_ui(
        attribute_name='cluster_log_conf.path',
        _placeholder='/dbfs/cluster-logs',
    )

def cluster_log_conf_region():
    set_attribute_description('The region of the log files, if using cloud storage.')
    options = st.session_state['regions']
    gen_string_attribute_ui(
        attribute_name='cluster_log_conf.region',
        _placeholder=options[0] if options else 'us-east-1',
        _options=options,
    )

def cluster_log_conf_type():
    set_attribute_description('The type of log destination.')
    options = [
        'S3',
        'VOLUMES',
        'DBFS',
        'NONE',
    ]
    gen_string_attribute_ui(
        attribute_name='cluster_log_conf.type',
        _options=options,
        _placeholder=options[0] if options else 'NONE',
    )

def cluster_name():
    set_attribute_description('The name of the cluster.')
    gen_string_attribute_ui(
        attribute_name='cluster_name',
        _placeholder='my-cluster',
    )

def data_security_mode():
    options = [
        'NONE',
        'SINGLE_USER',
        'USER_ISOLATION',
        # Following are deprecated:
        # 'LEGACY_TABLE_ACL',
        # 'LEGACY_PASSTHROUGH',
        # 'LEGACY_SINGLE_USER',
        # 'LEGACY_SINGLE_USER_STANDARD',
    ]
    set_attribute_description("""
        Sets the access mode of the cluster. Unity Catalog requires `SINGLE_USER` or 
        `USER_ISOLATION` (Standard access mode in the UI). A value of `NONE` means no
        security features are enabled.

        [Learn more](https://docs.databricks.com/aws/en/compute/configure#access-modes) about data security modes.  
    """)
    gen_string_attribute_ui(
        attribute_name='data_security_mode',
        _options=options,
        _placeholder='SINGLE_USER',
    )

def docker_image_basic_auth_password():
    set_attribute_description('The password for the Databricks Container Services image basic authentication.')
    gen_string_attribute_ui(
        attribute_name='docker_image.basic_auth.password',
        _placeholder='password',
    )

def docker_image_basic_auth_username():
    set_attribute_description('The user name for the Databricks Container Services image basic authentication.')
    gen_string_attribute_ui(
        attribute_name='docker_image.basic_auth.username',
        _placeholder='username',
    )

def docker_image_url():
    set_attribute_description('Controls the Databricks Container Services image URL. When hidden, removes the Databricks Container Services section from the UI.')
    gen_string_attribute_ui(
        attribute_name='docker_image.url',
        _placeholder='docker.io/...',
    )

def driver_node_type_id():
    set_attribute_description('The node type of the driver.')
    options = st.session_state['node_types']
    gen_string_attribute_ui(
        attribute_name='driver_node_type_id',
        _options=options,
        _placeholder=options[0] if options else 'i3.xlarge',
    )

def node_type_id():
    set_attribute_description('The node type of the worker.')
    options = st.session_state['node_types']
    gen_string_attribute_ui(
        attribute_name='node_type_id',
        _options=options,
        _placeholder=options[0] if options else 'i3.xlarge',
    )

def instance_pool_id():
    set_attribute_description('''
        Controls the pool used by worker nodes if `driver_instance_pool_id` is also defined,
        or for all cluster nodes otherwise. If you use pools for worker nodes, you must also
        use pools for the driver node. When hidden, removes pool selection from the UI.
    ''')
    options = list(st.session_state['instance_pools'].keys())
    gen_string_attribute_ui(
        attribute_name='instance_pool_id',
        _options=options,
        _placeholder=options[0] if options else '...',
        _format_func=lambda x: f"{st.session_state['instance_pools'][x]} ({x})",
    )

def num_workers():
    set_attribute_description('The number of worker nodes in the cluster.')
    gen_number_attribute_ui(
        attribute_name='num_workers',
        _min_value=0,
        _max_value=100000,
        _default_value=1,
    )

def runtime_engine():
    set_attribute_description('Determines whether the cluster uses Photon or not. Possible values are `PHOTON` or `STANDARD`.')
    options = ['STANDARD', 'PHOTON']
    gen_string_attribute_ui(
        attribute_name='runtime_engine',
        _options=options,
        _placeholder=options[0] if options else 'STANDARD',
    )

def single_user_name():
    set_attribute_description('The name of the single user.')
    gen_string_attribute_ui(
        attribute_name='single_user_name',
        _placeholder='user@example.com',
    )

def dbus_per_hour():
    set_attribute_description("""
        Calculated attribute representing the maximum DBUs a resource can use
        on an hourly basis including the driver node. This metric is a direct way
        to control cost at the individual compute level. Use with range limitation.
    """)
    gen_number_attribute_ui(
        attribute_name='dbus_per_hour',
        _min_value=1,
        _max_value=1000000,
        _default_value=10,
    )

def cluster_type():
    set_attribute_description("""
        Represents the type of cluster that can be created.
        Use with range limitation.
                              
        Allow or block specified types of compute to be created from the policy.
        If the all-purpose value is not allowed, the policy is not shown in the all-purpose create compute UI.
        If the job value is not allowed, the policy is not shown in the create job compute UI.
    """)
    options = ['all-purpose', 'job', 'dlt']
    gen_string_attribute_ui(
        attribute_name='cluster_type',
        _options=options,
        _placeholder=options[0] if options else 'all-purpose',
    )

def enable_elastic_disk():
    set_attribute_description('Controls whether the cluster uses autoscaling local disk.')
    gen_boolean_attribute_ui(
        attribute_name='enable_elastic_disk',
        default_value=False,
    )

def enable_local_disk_encryption():
    set_attribute_description('Controls whether the cluster uses local disk encryption.')
    gen_boolean_attribute_ui(
        attribute_name='enable_local_disk_encryption',
        default_value=False,
    )

def workload_type_jobs():
    set_attribute_description('''
        Defines whether the compute resource can be used for jobs. 
        See [Prevent compute from being used with jobs](https://docs.databricks.com/aws/en/admin/clusters/policy-definition#workload).
    ''')
    gen_boolean_attribute_ui(
        attribute_name='workload_type.clients.jobs',
        default_value=True,
    )

def workload_type_notebooks():
    set_attribute_description('''
        Defines whether the compute resource can be used for notebooks. 
        See [Prevent compute from being used with notebooks](https://docs.databricks.com/aws/en/admin/clusters/policy-definition#workload).
    ''')
    gen_boolean_attribute_ui(
        attribute_name='workload_type.clients.notebooks',
        default_value=True,
    )

def custom_tags():
    set_attribute_description('''
        Defines a custom tag for the cluster.
        See [Custom tags](https://docs.databricks.com/aws/en/admin/clusters/policy-definition#custom-tags).
    ''')
    tag_name = st.text_input('Tag Name', placeholder='TagName')

    if tag_name:
        st.session_state['override_attribute_name_select'] = f'custom_tags.{tag_name}'
        gen_string_attribute_ui(
            attribute_name=f'custom_tags.{tag_name}',
            _placeholder='TagValue',
        )

def spark_conf():
    set_attribute_description('''
        Control specific Spark configuration values.
    ''')
    conf_key = st.text_input('Spark Conf Key', placeholder='spark.executor.memory')
    if conf_key:
        st.session_state['override_attribute_name_select'] = f'spark_conf.{conf_key}'
        gen_string_attribute_ui(
            attribute_name=f'spark_conf.{conf_key}',
            _placeholder='8g',
        )

def spark_env_vars():
    set_attribute_description('''
        Control specific Spark environment variable.
    ''')
    env_var = st.text_input('Spark Env Var', placeholder='SPARK_ENV_VAR')
    if env_var:
        st.session_state['override_attribute_name_select'] = f'spark_env_vars.{env_var}'
        gen_string_attribute_ui(
            attribute_name=f'spark_env_vars.{env_var}',
            _placeholder='Value',
        )

def ssh_public_keys():
    set_attribute_description('Enforce authorized SSH keys for cluster access.')
    gen_array_string_attribute_ui('ssh_public_keys.*')

def init_scripts_workspace_destination():
    set_attribute_description('The workspace path of the init script.')
    gen_array_string_attribute_ui('init_scripts.*.workspace.destination')

def init_scripts_volumes_destination():
    set_attribute_description('The volume path of the init script.')
    gen_array_string_attribute_ui('init_scripts.*.volumes.destination')

def init_scripts_s3_destination():
    set_attribute_description('The S3 path of the init script.')
    gen_array_string_attribute_ui('init_scripts.*.s3.destination')

def init_scripts_file_destination():
    set_attribute_description('The file path of the init script.')
    gen_array_string_attribute_ui('init_scripts.*.file.destination')

def init_scripts_s3_region():
    set_attribute_description('The region of the S3 path of the init script.')
    gen_array_string_attribute_ui('init_scripts.*.s3.region')

# ===== Attribute UI Functions Map
supported_attributes = {
    'autoscale.max_workers': autoscale_max_workers,
    'autoscale.min_workers': autoscale_min_workers,
    'autotermination_minutes': autotermination_minutes,
    'aws_attributes.availability': aws_attributes_availability,
    'aws_attributes.ebs_volume_count': aws_attributes_ebs_volume_count,
    'aws_attributes.ebs_volume_size': aws_attributes_ebs_volume_size,
    'aws_attributes.ebs_volume_type': aws_attributes_ebs_volume_type,
    'aws_attributes.first_on_demand': aws_attributes_first_on_demand,
    'aws_attributes.instance_profile_arn': aws_attributes_instance_profile_arn,
    'aws_attributes.spot_bid_price_percent': aws_attributes_spot_bid_price_percent,
    'aws_attributes.zone_id': aws_attributes_zone_id,
    'cluster_log_conf.path': cluster_log_conf_path,
    'cluster_log_conf.region': cluster_log_conf_region,
    'cluster_log_conf.type': cluster_log_conf_type,
    'cluster_name': cluster_name,
    'data_security_mode': data_security_mode,
    'docker_image.basic_auth.password': docker_image_basic_auth_password,
    'docker_image.basic_auth.username': docker_image_basic_auth_username,
    'docker_image.url': docker_image_url,
    'driver_node_type_id': driver_node_type_id,
    'node_type_id': node_type_id,
    'instance_pool_id': instance_pool_id,
    'num_workers': num_workers,
    'runtime_engine': runtime_engine,
    'single_user_name': single_user_name,
    'spark_version': spark_version,
    'dbus_per_hour': dbus_per_hour,
    'cluster_type': cluster_type,
    'enable_elastic_disk': enable_elastic_disk,
    'enable_local_disk_encryption': enable_local_disk_encryption,
    'workload_type.clients.jobs': workload_type_jobs,
    'workload_type.clients.notebooks': workload_type_notebooks,
    'custom_tags': custom_tags,
    'spark_conf.*': spark_conf,
    'spark_env_vars.*': spark_env_vars,
    'ssh_public_keys.*': ssh_public_keys,
    'init_scripts.*.workspace.destination': init_scripts_workspace_destination,
    'init_scripts.*.volumes.destination': init_scripts_volumes_destination,
    'init_scripts.*.s3.destination': init_scripts_s3_destination,
    'init_scripts.*.file.destination': init_scripts_file_destination,
    'init_scripts.*.s3.region': init_scripts_s3_region,
}
