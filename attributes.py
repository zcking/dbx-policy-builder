import streamlit as st
from databricks.sdk.environments import Cloud
from typing import Callable, Any
from collections import OrderedDict

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
        at = st.session_state[f'{attribute_name}__attribute_type']
        if at in ('fixed', 'forbidden'):
            # Fixed and forbidden attributes don't make sense to set a default value or make optional
            st.session_state['toggle_options'] = ['Hide from UI']
        elif at in ('allowlist', 'blocklist'):
            st.session_state['toggle_options'] = ['Set Default Value', 'Make Optional']
        elif at in ('regex', 'range', 'unlimited'):
            st.session_state['toggle_options'] = ['Set Default Value', 'Make Optional']
        else:
            st.session_state['toggle_options'] = []

    if st.session_state.get('attribute_description'):
        st.write('Description')
        st.write(st.session_state['attribute_description'])

    if st.session_state['cloud'] == Cloud.AWS:
        policy_types_docs_url = "https://docs.databricks.com/aws/en/admin/clusters/policy-definition#supported-policy-types"
    elif st.session_state['cloud'] == Cloud.GCP:
        policy_types_docs_url = "https://docs.databricks.com/gcp/en/admin/clusters/policy-definition#supported-policy-types"
    elif st.session_state['cloud'] == Cloud.AZURE:
        policy_types_docs_url = "https://learn.microsoft.com/en-us/azure/databricks/admin/clusters/policy-definition#supported-policy-types"

    attribute_type = st.radio(
        'Type',
        options=[option for option in options if option is not None],
        key=f'{attribute_name}__attribute_type',
        on_change=_handle_attribute_type_change,
        horizontal=True,
        help=f"""Select the policy type for this attribute. See 
        [Policy Types]({policy_types_docs_url})
        for more information.""",
    )
    st.session_state['inputs']['type'] = attribute_type

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

def spark_version():
    # Set up the default value input logic
    # options = [
    #     'auto:latest-lts',
    #     'auto:latest',
    #     'auto:latest-ml',
    #     'auto:latest-lts-ml',
    #     'auto:prev-major',
    #     'auto:prev-major-ml',
    #     'auto:prev-lts',
    #     'auto:prev-lts-ml',
    #     '** Specify version (e.g. 15.3.x-scala2.12)'
    # ]
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
        # if fixed_value == options[-1]:
        #     spark_versions = st.session_state['spark_versions']
        #     spark_version_input = st.selectbox(
        #         'Spark Version',
        #         options=list(spark_versions.keys()),
        #         key="spark_version__fixed_value_input",
        #         index=None,
        #         format_func=lambda x: spark_versions[x],
        #     )
        #     st.session_state['inputs']['value'] = spark_version_input
        # elif fixed_value:
        #     st.session_state['inputs']['value'] = fixed_value

def autoscale_min_workers():
    _attribute_type(
        'autoscale.min_workers',
        range=True,
        allow_list=False,
        block_list=False,
        regex=False,
        default_value_input=lambda: st.number_input('Default Value', min_value=0),
    )

    if st.session_state['inputs']['type'] == 'range':
        col1, col2 = st.columns(2)
        with col1:
            min_value = st.number_input('Min Value', min_value=0, max_value=100000, key='autoscale.min_workers__min_value')
        with col2:
            max_value = st.number_input('Max Value', min_value=min_value, max_value=100000, key='autoscale.min_workers__max_value')
        st.session_state['inputs']['minValue'] = min_value
        st.session_state['inputs']['maxValue'] = max_value
    elif st.session_state['inputs']['type'] == 'fixed':
        autoscale_min_workers_input = st.number_input('Fixed Value', min_value=0, max_value=100000, value=1)
        st.session_state['inputs']['value'] = autoscale_min_workers_input

def autoscale_max_workers():
    _attribute_type(
        'autoscale.max_workers',
        range=True,
        allow_list=False,
        block_list=False,
        regex=False,
        default_value_input=lambda: st.number_input('Default Value', min_value=0),
    )

    if st.session_state['inputs']['type'] == 'range':
        col1, col2 = st.columns(2)
        with col1:
            min_value = st.number_input('Min Value', min_value=0, max_value=100000, key='autoscale.max_workers__min_value')
        with col2:
            max_value = st.number_input('Max Value', min_value=min_value, max_value=100000, key='autoscale.max_workers__max_value')
        st.session_state['inputs']['minValue'] = min_value
        st.session_state['inputs']['maxValue'] = max_value
    elif st.session_state['inputs']['type'] == 'fixed':
        autoscale_max_workers_input = st.number_input('Fixed Value', min_value=0, max_value=100000, value=4)
        st.session_state['inputs']['value'] = autoscale_max_workers_input

def autotermination_minutes():
    set_attribute_description('A value of 0 represents no auto termination. When hidden, removes the auto termination checkbox and value input from the UI.')
    at = _attribute_type(
        'autotermination_minutes',
        range=True,
        allow_list=False,
        block_list=False,
        regex=False,
        default_value_input=lambda: st.number_input('Default Value', min_value=10, max_value=43200, value=60),
    )

    if at == 'range':
        col1, col2 = st.columns(2)
        with col1:
            min_value = st.number_input('Min Value', min_value=10, max_value=43200, key='autotermination_minutes__min_value')
        with col2:
            max_value = st.number_input('Max Value', min_value=min_value, max_value=43200, key='autotermination_minutes__max_value')
        st.session_state['inputs']['minValue'] = min_value
        st.session_state['inputs']['maxValue'] = max_value
    elif at == 'fixed':
        autotermination_minutes_input = st.number_input('Fixed Value', min_value=10, max_value=43200, value=60)
        st.session_state['inputs']['value'] = autotermination_minutes_input

def aws_attributes_availability():
    set_attribute_description('Controls AWS availability (SPOT, ON_DEMAND, or SPOT_WITH_FALLBACK)')
    options = ['ON_DEMAND', 'SPOT', 'SPOT_WITH_FALLBACK']

    at = _attribute_type(
        'aws_attributes.availability',
        regex=False,
        default_value_input=lambda: st.selectbox('Default Value', options=options, index=None),
    )
    if at in ('allowlist', 'blocklist'):
        values = st.multiselect(
            'Values',
            options=options,
            key='aws_attributes.availability__values',
        )
        st.session_state['inputs']['values'] = values
    elif at == 'fixed':
        aws_attributes_availability_input = st.selectbox('Fixed Value', options=options, index=None)
        st.session_state['inputs']['value'] = aws_attributes_availability_input

def aws_attributes_ebs_volume_count():
    set_attribute_description('The number of AWS EBS volumes.')

    at = _attribute_type(
        'aws_attributes.ebs_volume_count',
        range=True,
        allow_list=False,
        block_list=False,
        regex=False,
        default_value_input=lambda: st.number_input('Default Value', min_value=1, max_value=28, value=1),
    )
    if at == 'range':
        col1, col2 = st.columns(2)
        with col1:
            min_value = st.number_input('Min Value', min_value=1, max_value=28, key='aws_attributes.ebs_volume_count__min_value')
        with col2:
            max_value = st.number_input('Max Value', min_value=min_value, max_value=28, key='aws_attributes.ebs_volume_count__max_value')
        st.session_state['inputs']['minValue'] = min_value
        st.session_state['inputs']['maxValue'] = max_value
    else:
        aws_attributes_ebs_volume_count_input = st.number_input('Fixed Value', min_value=1, max_value=28, value=1)
        st.session_state['inputs']['value'] = aws_attributes_ebs_volume_count_input

def aws_attributes_ebs_volume_size():
    set_attribute_description('The size (in GiB) of AWS EBS volumes.')
    at = _attribute_type(
        'aws_attributes.ebs_volume_size',
        range=True,
        allow_list=False,
        block_list=False,
        regex=False,
        default_value_input=lambda: st.number_input('Default Value', min_value=1, max_value=16384, value=100),
    )
    if at == 'range':
        col1, col2 = st.columns(2)
        with col1:
            min_value = st.number_input('Min Value', min_value=1, max_value=16384, key='aws_attributes.ebs_volume_size__min_value')
        with col2:
            max_value = st.number_input('Max Value', min_value=min_value, max_value=16384, key='aws_attributes.ebs_volume_size__max_value')
        st.session_state['inputs']['minValue'] = min_value
        st.session_state['inputs']['maxValue'] = max_value
    elif at == 'fixed':
        aws_attributes_ebs_volume_size_input = st.number_input('Fixed Value', min_value=1, max_value=16384, value=100)
        st.session_state['inputs']['value'] = aws_attributes_ebs_volume_size_input

# ===== Attribute UI Functions Map
supported_attributes = {
    'autoscale.max_workers': autoscale_max_workers,
    'autoscale.min_workers': autoscale_min_workers,
    'autotermination_minutes': autotermination_minutes,
    'aws_attributes.availability': aws_attributes_availability,
    'aws_attributes.ebs_volume_count': aws_attributes_ebs_volume_count,
    'aws_attributes.ebs_volume_size': aws_attributes_ebs_volume_size,
    # ...
    'spark_version': spark_version,
}