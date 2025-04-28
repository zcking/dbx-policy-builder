import streamlit as st
from databricks.sdk.environments import Cloud
from typing import Callable, Any
from collections import OrderedDict

# ===== Attribute Logic Helpers =====

def set_toggle_options(attribute_name: str):
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

def aws_attributes_first_on_demand():
    set_attribute_description('Controls how many instances are requested on-demand before requesting SPOT instances.')
    gen_number_attribute_ui(
        attribute_name='aws_attributes.first_on_demand',
        _min_value=0,
        _max_value=100000,
        _default_value=1,
    )

# ===== Attribute UI Functions Map
supported_attributes = {
    'autoscale.max_workers': autoscale_max_workers,
    'autoscale.min_workers': autoscale_min_workers,
    'autotermination_minutes': autotermination_minutes,
    'aws_attributes.availability': aws_attributes_availability,
    'aws_attributes.ebs_volume_count': aws_attributes_ebs_volume_count,
    'aws_attributes.ebs_volume_size': aws_attributes_ebs_volume_size,
    'aws_attributes.first_on_demand': aws_attributes_first_on_demand,
    # ...
    'spark_version': spark_version,
}