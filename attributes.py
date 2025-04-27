import streamlit as st
from typing import Callable, Any

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
        if st.session_state[f'{attribute_name}__attribute_type'] in ('fixed', 'forbidden'):
            # Fixed and forbidden attributes don't make sense to set a default value or make optional
            st.session_state['toggle_options'] = ['Hide from UI']
        else:
            st.session_state['toggle_options'] = ['Set Default Value', 'Make Optional', 'Hide from UI']

    if st.session_state.get('attribute_description'):
        st.write('Description')
        st.write(st.session_state['attribute_description'])

    attribute_type = st.radio(
        'Type',
        options=[option for option in options if option is not None],
        key=f'{attribute_name}__attribute_type',
        on_change=_handle_attribute_type_change,
        horizontal=True,
    )
    st.session_state['inputs']['type'] = attribute_type

    if default_value_input:
        toggle_options = st.session_state['toggle_options']
        toggles = st.pills(
            label='Features',
            key=f'{attribute_name}__toggle_group',
            options=toggle_options,
            selection_mode='multi',
        )

        # Set Default Value
        if 'Set Default Value' in toggles:
            default_value = default_value_input()
            st.session_state['inputs']['defaultValue'] = default_value
        elif 'defaultValue' in st.session_state['inputs']:
            st.session_state['inputs'].pop('defaultValue')

        # Optional
        st.session_state['inputs']['isOptional'] = 'Make Optional' in toggles

        # Hide from UI
        st.session_state['inputs']['hidden'] = 'Hide from UI' in toggles
    return attribute_type

def spark_version():
    # Set up the default value input logic
    options = [
        'auto:latest',
        'auto:latest-ml',
        'auto:latest-lts-ml',
        'auto:prev-major',
        'auto:prev-major-ml',
        'auto:prev-lts',
        'auto:prev-lts-ml',
        '** Specify version (e.g. 15.3.x-scala2.12)'
    ]

    def _default_value_input():
        default_value = st.selectbox(
            'Default Value',
            options=options,
            key="spark_version__default_value_select",
            index=None,
        )
        if default_value == options[-1]:
            spark_version_input = st.text_input('Spark Version', placeholder='15.3.x-scala2.12')
            return spark_version_input
        elif default_value:
            return default_value

    _attribute_type('spark_version', _default_value_input)

    # If the selection type is `allowlist` or `blocklist`, we need to allow the user to add multiple values
    # TODO: can we dynamically fetch all the possible values?
    if st.session_state['inputs']['type'] in ('allowlist', 'blocklist'):
        st.subheader(st.session_state['inputs']['type'].title() + ' Values')
        values = st.data_editor(
            data=[
                {"spark_version": options[0]},
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
    elif st.session_state['inputs']['type'] in ('fixed', 'forbidden'):
        fixed_value = st.selectbox(
            'Fixed Value',
            options=options,
            key="spark_version__fixed_value_select",
            index=None,
        )
        if fixed_value == options[-1]:
            spark_version_input = st.text_input('Spark Version', placeholder='15.3.x-scala2.12')
            st.session_state['inputs']['value'] = spark_version_input
        elif fixed_value:
            st.session_state['inputs']['value'] = fixed_value

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
    elif st.session_state['inputs']['type'] in ('fixed', 'forbidden'):
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
    elif st.session_state['inputs']['type'] in ('fixed', 'forbidden'):
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
            min_value = st.number_input('Min Value', min_value=0, max_value=43200, key='autotermination_minutes__min_value')
        with col2:
            max_value = st.number_input('Max Value', min_value=min_value, max_value=43200, key='autotermination_minutes__max_value')
        st.session_state['inputs']['minValue'] = min_value
        st.session_state['inputs']['maxValue'] = max_value
    elif at in ('fixed', 'forbidden'):
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
    else:
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
    else:
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