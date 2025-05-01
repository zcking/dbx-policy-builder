# Databricks Cluster Policy Builder

A user-friendly web application for creating and managing Databricks cluster policies. This application provides an intuitive interface for configuring cluster policies, including support for policy families, attribute overrides, and real-time policy preview.

![demo](https://github.com/zcking/zcking.github.io/blob/cdn/images/dbx-policy-builder-demo-optimized.gif?raw=true)

## Features

- Interactive UI for building cluster policies
- Support for all Databricks cluster policy attributes
- Real-time policy preview
- Policy family support with override capabilities
- Search and filter existing policies
- Clone existing policies
- Local development and Databricks Apps deployment support

## Prerequisites

- Python 3.11 or above, and [uv](docs.astral.sh/uv/)
- Databricks CLI (v0.233.0 or above)

## Running Locally

1. Clone the repository:
```bash
git clone <repository-url>
cd dbx-policy-builder
```

2. Install the required dependencies:
```bash
uv venv && uv sync
```

3. Run the application:
```bash
streamlit run app.py
```

The application will be available at `http://localhost:8501`

## Deploying as a Databricks App

1. Deploy the app using the Databricks CLI:
```bash
databricks apps deploy dbx-policy-builder
```

The app will be available in your Databricks workspace under the Compute >> Apps section. It may take a few minutes for the App compute to spin up the first time.

> **Note:** once your app is deployed, you should note the service principal ID that gets generated for it. This can be found in the workspace UI. You may then add this policy to your `admins` group in the workspace; this will allow the app to access all policies. In the future we will work to add [On-Behalf-Of User Tokens](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/app-development#add-on-behalf-of-user-authorization-to-a-databricks-app) so stay tuned!

## Usage

1. **Creating a New Policy**
   - Click "Reset Policy" to start fresh
   - Configure policy name (required), description, and max clusters per user
   - Select attributes to configure from the dropdown
   - Fill out the settings for the attribute, then "Add to Policy" to add to your draft
   - Preview the policy JSON draft as you go
   - Once satisfied, save the policy to your workspace

2. **Editing an Existing Policy**
   - Select a policy from the sidebar
   - Modify attributes as needed
   - Save changes to update the policy

3. **Using Default Policy Families**
   - Select a policy family from the dropdown
   - Configure overrides for specific attributes
   - The base family definition will be inherited

4. **Cloning a Policy**
   - Select a policy from the sidebar
   - Click "Clone Policy"
   - Make any changes to the cloned policy if necessary
   - Save the new policy

## Contributing

Use GitHub issues to submit feature requests or report any bugs. I will try to get to these as soon as possible.

If you would like to contibute yourself, please still start by submitting a GitHub issue, and state your interest in contributing.

General guidance for code contributions following issue discussion:  

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## Support

For issues and feature requests, please create an issue in the repository.
