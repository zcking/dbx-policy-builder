from databricks.sdk.core import Config
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Policy, PolicyFamily
import streamlit as st
from streamlit_extras.st_keyup import st_keyup
import json
from collections import OrderedDict

from attributes import supported_attributes


# Databricks config
cfg = Config()

# Initialize streamlit
st.set_page_config(layout="wide")

st.session_state['cloud'] = cfg.environment.cloud
if 'inputs' not in st.session_state:
    st.session_state['inputs'] = {}
if 'definition' not in st.session_state:
    st.session_state['definition'] = {}
if 'overrides' not in st.session_state:
    st.session_state['overrides'] = {}
if 'cache_cursor' not in st.session_state:
    st.session_state['cache_cursor'] = 0
if 'toggle_options' not in st.session_state:
    st.session_state['toggle_options'] = ['Hide from UI']

def clear_inputs():
    st.session_state['inputs'] = {}


# ===== Logic =====

@st.cache_resource(show_spinner='Talking to your Databricks workspace...')
def workspace_client() -> WorkspaceClient:
    user_token = st.context.headers.get('X-Forwarded-Access-Token')
    return WorkspaceClient(
        host=cfg.host,
        token=user_token
    )

@st.cache_data(ttl='1 hour', show_spinner='Discovering cluster policies...')
def list_cluster_policies(cache_cursor: int) -> list[Policy]:
    """List all cluster policies in the workspace"""
    w = workspace_client()
    return w.cluster_policies.list()

@st.cache_data(ttl='24 hours', show_spinner='Loading policy families...')
def load_policy_families() -> list[PolicyFamily]:
    """List all policy families in the workspace"""
    w = workspace_client()
    return list(w.policy_families.list())

@st.cache_data(ttl='24 hours', show_spinner='Loading Available Spark Versions...')
def load_available_spark_versions() -> list[str]:
    """List all available spark versions in the workspace"""
    w = workspace_client()
    versions = w.clusters.spark_versions().versions
    st.session_state['spark_versions'] = OrderedDict({v.key: v.name for v in versions})
    return st.session_state['spark_versions']

if 'spark_versions' not in st.session_state:
    st.session_state['spark_versions'] = load_available_spark_versions()

def add_inputs_to_definition():
    # When using a Family, the definition itself is not editable, but the overrides are.
    if st.session_state.get('policy_family_id'):
        st.session_state['overrides'][st.session_state['attribute_name_select']] = st.session_state['inputs']
    else:
        st.session_state['definition'][st.session_state['attribute_name_select']] = st.session_state['inputs']
    attribute_name = st.session_state['attribute_name_select']
    st.session_state.pop(f'{attribute_name}__attribute_type')
    st.session_state['attribute_name_select'] = None
    clear_inputs()

def load_policy(policy: Policy):
    clear_inputs()
    # Make sure we have the most recent data for the policy
    with st.spinner('Loading policy...'):
        w = workspace_client()
        policy = w.cluster_policies.get(policy.policy_id)

    st.session_state['definition'] = json.loads(policy.definition)
    if policy.policy_family_definition_overrides:
        st.session_state['overrides'] = json.loads(policy.policy_family_definition_overrides)
    else:
        st.session_state['overrides'] = {}
    st.session_state['editing_policy'] = policy
    st.session_state['max_clusters_per_user'] = policy.max_clusters_per_user
    st.session_state['policy_name'] = policy.name
    st.session_state['policy_description'] = policy.description
    st.session_state['policy_family_id'] = policy.policy_family_id

def clone_policy():
    cloned_policy_name = st.session_state['editing_policy'].name
    st.session_state['definition'] = json.loads(st.session_state['editing_policy'].definition)
    st.session_state['editing_policy'] = None
    st.info(
        f'**{cloned_policy_name}** cloned. You may continue making changes to the Policy, and click **Save Policy** to create a new policy without affecting the original.',
        icon=':material/info:',
    )

# ===== Toast Notifications =====

if st.session_state.get('newly_created_policy_id'):
    new_policy_url = f"{cfg.host}/compute/policies/{st.session_state['newly_created_policy_id']}"
    if st.session_state.get('editing_policy'):
        st.success(
            f"Policy updated: [{st.session_state['editing_policy'].name}]({new_policy_url})",
            icon=':material/check_circle:',
        )
    else:
        st.success(
            f"Policy created: [{st.session_state['newly_created_policy_name']}]({new_policy_url})",
            icon=':material/check_circle:',
        )
    st.session_state['editing_policy'] = None
    st.session_state['newly_created_policy_id'] = None
    st.session_state['newly_created_policy_name'] = None
    st.session_state['definition'] = {}

if st.session_state.get('editing_policy'):
    existing_policy_url = f"{cfg.host}/compute/policies/{st.session_state['editing_policy'].policy_id}"
    st.info(f"You are editing [{st.session_state['editing_policy'].name}]({existing_policy_url})", icon=':material/info:')

# ===== UI =====

# Each policy has a definition that is a json object of zero or more attributes.
# Each attribute has a `type`. It may also have a `defaultValue`, `hidden`, `isOptional`,
# `value`, `values`, `pattern`, `minValue`, `maxValue` depending on the type.

# Popup dialog to create/submit the Policy to the workspace
@st.dialog('Create/Update Policy')
def create_policy_dialog():
    editing_policy = st.session_state.get('editing_policy')
    if editing_policy:
        existing_policy_url = f"{cfg.host}/compute/policies/{editing_policy.policy_id}"
        _message = f"""
        By submitting, the [{editing_policy.name}]({existing_policy_url}) policy will be updated.
        """
    else:
        _message = f"""
        By submitting, the policy will be created in the workspace at:
        {cfg.host}/compute/policies
        """
    st.write(_message)

    st.write('#### View Policy JSON:')
    st.json(st.session_state['definition'], expanded=False)

    # Input the policy name
    policy_name = st.text_input(
        'Policy Name',
        placeholder='My Policy',
        key='final_policy_name',
        value=st.session_state.get('policy_name'),
    )
    policy_description = st.text_input(
        'Policy Description',
        placeholder='My Policy Description',
        key='final_policy_description',
        value=st.session_state.get('policy_description'),
    )
    max_clusters_per_user = st.number_input(
        'Max Clusters Per User',
        min_value=0,
        max_value=10000,
        help='The maximum number of clusters a user can have active with this policy at a time.',
        key='final_max_clusters_per_user',
        value=st.session_state.get('max_clusters_per_user'),
    )
    if max_clusters_per_user == 0:
        max_clusters_per_user = None

    # Add a button to create the policy
    button_label = 'Create Policy' if not editing_policy else 'Update Policy'
    if st.button(button_label, key='submit_create_policy_button', use_container_width=True, disabled=not policy_name):
        w = workspace_client()
        request_args = {
            'name': policy_name,
            'max_clusters_per_user': max_clusters_per_user,
            'description': policy_description,
        }
        if st.session_state.get('policy_family_id'):
            request_args['policy_family_id'] = st.session_state['policy_family_id']
            request_args['policy_family_definition_overrides'] = json.dumps(st.session_state['overrides'])
        else:
            request_args['definition'] = json.dumps(st.session_state['definition'])

        # Make the API call to create or update the policy
        if editing_policy:
            request_args['policy_id'] = editing_policy.policy_id
            request_args['libraries'] = editing_policy.libraries
            w.cluster_policies.edit(**request_args)
            st.session_state['newly_created_policy_id'] = editing_policy.policy_id
        else:
            resp = w.cluster_policies.create(**request_args)
            st.session_state['newly_created_policy_id'] = resp.policy_id

        # Refresh the policy list
        st.session_state['newly_created_policy_name'] = policy_name
        st.session_state['cache_cursor'] += 1
        st.rerun()

@st.dialog('Start New Policy')
def start_new_policy_dialog():
    # Ask the user to confirm that they want to start a new policy
    st.write('Are you sure you want to start building a new policy? Any unsaved changes to the current editor will be lost.')
    if st.button('Confirm', use_container_width=True, type='primary'):
        clear_inputs()
        st.session_state['definition'] = {}
        st.session_state['editing_policy'] = None
        st.session_state['policy_family_id'] = None
        st.session_state['overrides'] = {}
        st.session_state['policy_name'] = None
        st.session_state['policy_description'] = None
        st.session_state['max_clusters_per_user'] = None
        st.rerun()
    if st.button('Cancel', use_container_width=True, type='secondary'):
        st.rerun() # nothing, just closes the dialog

def editor_ui_container():
    st.write('#### :material/tune: Edit Attribute')
    st.selectbox(
        'Select an Attribute to Configure',
        options=list(supported_attributes.keys()),
        key='attribute_name_select',
        on_change=clear_inputs,
        placeholder='Select an attribute to configure',
        index=None,
    )

    # Render the corresponding UI input elements based on which attribute is selected
    if st.session_state.get('attribute_name_select'):
        supported_attributes[st.session_state['attribute_name_select']]()

    st.button(
        'Add to Policy',
        on_click=add_inputs_to_definition,
        type='primary',
        disabled=not st.session_state.get('attribute_name_select') or not st.session_state.get('inputs'),
        help='Add the current attribute to the policy definition',
    )

def preview_policy_container():
    st.write('#### :material/draft: Policy Preview')
    if st.session_state['overrides']:
        st.write('###### Overrides')
        st.json(st.session_state['overrides'], expanded=True)
        st.write('###### Family Definition')
        st.json(st.session_state['definition'], expanded=False)
    else:
        st.json(st.session_state['definition'], expanded=True)

st.title('Databricks Cluster Policy Builder')
top_buttons = st.columns(5)
with top_buttons[0]:
    st.link_button(
        'Open Workspace',
        url=cfg.host,
        type='secondary',
        icon=':material/open_in_new:',
        help='Open the Databricks workspace in a new tab',
        use_container_width=True,
    )
with top_buttons[1]:
    st.button(
        'Reset Policy',
        on_click=start_new_policy_dialog,
        use_container_width=True,
        type='secondary',
        help='Start building a new policy from scratch',
        icon=':material/restart_alt:',
    )
with top_buttons[2]:
    st.button(
        'Clone Policy',
        type='secondary',
        use_container_width=True,
        help='Clone an existing policy definition into a new policy',
        icon=':material/content_copy:',
        disabled=not st.session_state.get('editing_policy') or not st.session_state.get('definition'),
        on_click=clone_policy,
    )

# Top-level policy inputs
policy_cols = st.columns([0.3, 0.7])
with policy_cols[0]:
    st.text_input(
        'Name',
        placeholder='My Policy',
        key='policy_name',
        value=st.session_state.get('editing_policy').name if st.session_state.get('editing_policy') else None,
    )
    st.number_input(
        'Max Clusters Per User',
        min_value=0,
        max_value=10000,
        help='The maximum number of clusters a user can have active with this policy at a time. Set to 0 for no limit',
        key='max_clusters_per_user',
        value=st.session_state.get('editing_policy').max_clusters_per_user if st.session_state.get('editing_policy') else None,
    )
with policy_cols[1]:
    st.text_input(
        'Description',
        placeholder='My Policy Description',
        key='policy_description',
        value=st.session_state.get('editing_policy').description if st.session_state.get('editing_policy') else None,
    )
    policy_families = load_policy_families()
    family_option_labels = {p.policy_family_id: p.name for p in policy_families}
    family_options = list(family_option_labels.keys())
    st.selectbox(
        'Family',
        options=family_options,
        key='policy_family_id',
        help='Select a family to use as a base for the policy. The policy will inherit the family definition, but you can override any attributes.',
        index=family_options.index(st.session_state.get('policy_family_id')) if st.session_state.get('policy_family_id') else None,
        disabled=st.session_state.get('editing_policy').is_default if st.session_state.get('editing_policy') else False,
        format_func=lambda x: family_option_labels[x],
    )

# Sidebar
with st.sidebar:
    st.write('# :material/list: Cluster Policies')
    st.write('Select a policy to load its definition into the editor.')
    search_query = st_keyup("Policy Name/ID", placeholder="Type to search...", debounce=200)

    if st.button(
        'Refresh',
        use_container_width=True,
        type='primary',
        help='Refresh the list of policies from the workspace',
        icon=':material/refresh:',
    ):
        st.session_state['cache_cursor'] += 1
        st.rerun()

    with st.spinner('Loading policies...'):
        policies = list_cluster_policies(st.session_state['cache_cursor'])

    # Filter policies based on search query
    if search_query:
        search_query = search_query.lower()
        policies = [
            policy for policy in policies
            if search_query in policy.name.lower() or search_query in policy.policy_id.lower()
        ]
    
    for policy in policies:
        st.button(
            policy.name,
            on_click=load_policy,
            args=(policy,),
            use_container_width=True,
        )

main_col1, main_col2 = st.columns([0.6, 0.4], gap='small')
with main_col1:
    with st.container(border=True):
        editor_ui_container()

    if st.button(
        'Save Policy',
        type='primary',
        use_container_width=False,
        disabled=not st.session_state.get('definition'),
        help='Save the current policy definition to the workspace',
    ):
        create_policy_dialog()

with main_col2:
    with st.container(border=False):
        preview_policy_container()

# Show the session state for debugging
# st.json(st.session_state)
