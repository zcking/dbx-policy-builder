import streamlit as st

def _attribute_type(attribute_name: str, range: bool = False, allow_list: bool = True, 
                    block_list: bool = True, regex: bool = True, unlimited: bool = True) -> str:
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
    attribute_type = st.pills(
        'Type',
        options=[option for option in options if option is not None],
        default='fixed',
        key=f'{attribute_name}__attribute_type',
        on_change=lambda: st.session_state['inputs'].clear(),
    )
    st.session_state['inputs']['type'] = attribute_type
    return attribute_type

def spark_version():
    _attribute_type('spark_version')

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

    toggles = st.pills(
        label='',
        label_visibility='hidden',
        key='spark_version__toggle_group',
        options=[
            'Default Value',
            'Make Optional?',
        ],
        selection_mode='multi',
        default=['Default Value'],
        disabled=st.session_state['inputs']['type'] in ('fixed', 'forbidden'),
    )

    # Default value
    if 'Default Value' in toggles:
        default_value = st.selectbox(
            'Default Value',
            options=options,
            key="spark_version__default_value_select",
            index=None,
        )
        if default_value == options[-1]:
            spark_version_input = st.text_input('Spark Version', placeholder='15.3.x-scala2.12')
            st.session_state['inputs']['defaultValue'] = spark_version_input
        elif default_value:
            st.session_state['inputs']['defaultValue'] = default_value
    elif 'defaultValue' in st.session_state['inputs']:
        st.session_state['inputs'].pop('defaultValue')

    # Optional
    st.session_state['inputs']['isOptional'] = 'Make Optional?' in toggles

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
        st.subheader('Value')
        value_input = st.text_input('Value', placeholder='...')
        st.session_state['inputs']['value'] = value_input

def autoscale_min_workers():
    _attribute_type('autoscale.min_workers', range=True, allow_list=False, block_list=False, regex=False)

    toggles = st.pills(
        label='',
        label_visibility='hidden',
        key='autoscale.min_workers__toggle_group',
        options=[
            'Default Value',
            'Make Optional?',
        ],
        selection_mode='multi',
        default=['Default Value'],
        disabled=st.session_state['inputs']['type'] in ('fixed', 'forbidden'),
    )

    # Default value
    if 'Default Value' in toggles:
        default_value = st.number_input('Default Value', min_value=0)
        st.session_state['inputs']['defaultValue'] = default_value
    elif 'defaultValue' in st.session_state['inputs']:
        st.session_state['inputs'].pop('defaultValue')

    # Optional
    st.session_state['inputs']['isOptional'] = 'Make Optional?' in toggles

    if st.session_state['inputs']['type'] == 'range':
        col1, col2 = st.columns(2)
        with col1:
            min_value = st.number_input('Min Value', min_value=0, key='autoscale.min_workers__min_value')
        with col2:
            max_value = st.number_input('Max Value', min_value=0, key='autoscale.min_workers__max_value')
        st.session_state['inputs']['minValue'] = min_value
        st.session_state['inputs']['maxValue'] = max_value
    elif st.session_state['inputs']['type'] in ('fixed', 'forbidden'):
        autoscale_min_workers_input = st.number_input('Fixed Value', min_value=0)
        st.session_state['inputs']['value'] = autoscale_min_workers_input


def autoscale_max_workers():
    _attribute_type('autoscale.max_workers', range=True, allow_list=False, block_list=False, regex=False)

    toggles = st.pills(
        label='',
        label_visibility='hidden',
        key='autoscale.max_workers__toggle_group',
        options=[
            'Default Value',
            'Make Optional?',
        ],
        selection_mode='multi',
        default=['Default Value'],
        disabled=st.session_state['inputs']['type'] in ('fixed', 'forbidden'),
    )

    # Default value
    if 'Default Value' in toggles:
        default_value = st.number_input('Default Value', min_value=0)
        st.session_state['inputs']['defaultValue'] = default_value
    elif 'defaultValue' in st.session_state['inputs']:
        st.session_state['inputs'].pop('defaultValue')

    # Optional
    st.session_state['inputs']['isOptional'] = 'Make Optional?' in toggles

    if st.session_state['inputs']['type'] == 'range':
        col1, col2 = st.columns(2)
        with col1:
            min_value = st.number_input('Min Value', min_value=0, key='autoscale.max_workers__min_value')
        with col2:
            max_value = st.number_input('Max Value', min_value=0, key='autoscale.max_workers__max_value')
        st.session_state['inputs']['minValue'] = min_value
        st.session_state['inputs']['maxValue'] = max_value
    elif st.session_state['inputs']['type'] in ('fixed', 'forbidden'):
        autoscale_max_workers_input = st.number_input('Fixed Value', min_value=0)
        st.session_state['inputs']['value'] = autoscale_max_workers_input


# Define a map of attribute name to attribute function
supported_attributes = {
    'spark_version': spark_version,
    'autoscale.max_workers': autoscale_max_workers,
    'autoscale.min_workers': autoscale_min_workers,
}