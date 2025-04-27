from databricks.sdk.core import Config
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Policy
import streamlit as st
from streamlit_extras.st_keyup import st_keyup
import json
import attributes as attrs


# Databricks config
cfg = Config()

# Initialize streamlit
st.set_page_config(layout="wide")

if 'inputs' not in st.session_state:
    st.session_state['inputs'] = {}
if 'definition' not in st.session_state:
    st.session_state['definition'] = {}
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

def add_inputs_to_definition():
    st.session_state['definition'][st.session_state['attribute_name_select']] = st.session_state['inputs']
    st.session_state['attribute_name_select'] = None
    clear_inputs()

def load_policy(policy: Policy):
    clear_inputs()
    st.session_state['definition'] = json.loads(policy.definition)
    st.session_state['editing_policy'] = policy
    existing_policy_url = f"{cfg.host}/compute/policies/{st.session_state['editing_policy'].policy_id}"
    st.info(f"You are editing [{st.session_state['editing_policy'].name}]({existing_policy_url})")

def clone_policy():
    cloned_policy_name = st.session_state['editing_policy'].name
    st.session_state['definition'] = json.loads(st.session_state['editing_policy'].definition)
    st.session_state['editing_policy'] = None
    st.info(f'**{cloned_policy_name}** cloned. You may continue making changes to the Policy, and click **Save Policy** to create a new policy without affecting the original.')

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
        key='policy_name',
        value=editing_policy.name if editing_policy else None,
    )
    policy_description = st.text_input(
        'Policy Description',
        placeholder='My Policy Description',
        key='policy_description',
        value=editing_policy.description if editing_policy else None,
    )

    # Add a button to create the policy
    button_label = 'Create Policy' if not editing_policy else 'Update Policy'
    if st.button(button_label, key='submit_create_policy_button', use_container_width=True, disabled=not policy_name):
        w = workspace_client()
        if editing_policy:
            w.cluster_policies.edit(
                policy_id=editing_policy.policy_id,
                name=policy_name,
                description=policy_description,
                definition=json.dumps(st.session_state['definition'])
            )
            st.session_state['newly_created_policy_id'] = editing_policy.policy_id
        else:
            resp = w.cluster_policies.create(
                name=policy_name,
                description=policy_description,
                definition=json.dumps(st.session_state['definition'])
            )
            st.session_state['newly_created_policy_id'] = resp.policy_id

        st.session_state['newly_created_policy_name'] = policy_name
        # Refresh the policy list
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
        st.rerun()
    if st.button('Cancel', use_container_width=True, type='secondary'):
        st.rerun() # nothing, just closes the dialog

def main_ui_container():
    st.write('#### :material/tune: Edit Attribute')
    st.selectbox(
        'Attribute Name',
        options=attrs.supported_attributes.keys(),
        key='attribute_name_select',
        on_change=clear_inputs,
        placeholder='Select an attribute to configure',
        index=None,
    )

    # Render the corresponding UI input elements based on which attribute is selected
    if st.session_state.get('attribute_name_select'):
        attrs.supported_attributes[st.session_state['attribute_name_select']]()

    st.button(
        'Add to Policy',
        on_click=add_inputs_to_definition,
        type='primary',
        disabled=not st.session_state.get('attribute_name_select') or not st.session_state.get('inputs'),
    )

def preview_policy_container():
    st.write('#### :material/draft: Policy Preview')
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

# Sidebar
with st.sidebar:
    st.write('# :material/list: Cluster Policies')
    st.write('Select a policy to load its definition into the editor.')
    search_query = st_keyup("Policy Name/ID", placeholder="Type to search...", debounce=200)
    
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
        main_ui_container()

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


