import tempfile

from kbcstorage.client import Client
from keboola.component import CommonInterface


def _build_unique_tags(context: CommonInterface, additional_tags: list[str] = None) -> list[str]:
    """
    Build the unique tags for the artefact.
    Args:
        context: CommonInterface object
        additional_tags: List of additional tags to be added to the artefact

    Returns:
        List of unique tags
    """
    tags = [f'{context.environment_variables.component_id}-simulated-artefact',
            f'{context.environment_variables.project_id}-project_id',
            f'{context.environment_variables.config_id}-config_id',
            f'{context.environment_variables.config_row_id}-config_row_id',
            f'{context.environment_variables.branch_id}-branch_id']
    if additional_tags:
        tags.extend(additional_tags)
    return tags


def store_artefact(source_file_path: str, context: CommonInterface,
                   additional_tags: list[str] = None) -> None:
    """
    Store the artefact in the context with the given artefact_name.
    Args:
        context: CommonInterface object
        additional_tags: List of additional tags to be added to the artefact

    Returns:

    """
    tags = _build_unique_tags(context, additional_tags)
    client = Client(f'https://{context.environment_variables.stack_id}', context.environment_variables.token)
    client.files.upload_file(source_file_path, tags=tags, is_permanent=False)


def build_tags_query_filter(tags: list[str]) -> str:
    """
    Builds the tags query filter.
    Args:
        tags: List of tags

    Returns: Tags query filter
    """
    quoted_tags = [f'"{t}"' for t in tags]
    return f"tags:({'AND'.join(quoted_tags)})"


def get_artefact(artefact_file_name: str, context: CommonInterface,
                 additional_tags: list[str] = None) -> tuple[str, list[str]]:
    """
    Gets the artefact in the context with the given artefact_name. Note that the context must contain forwarded token.
    Args:
        artefact_file_name: Name of the artefact file
        context: CommonInterface object
        additional_tags: List of additional tags to be added to the artefact

    Returns: Resulting file path, None if the file does not exist

    """
    tags = _build_unique_tags(context, additional_tags)
    client = Client(f'https://{context.environment_variables.stack_id}', context.environment_variables.token)

    files = client.files.list(q=build_tags_query_filter(tags))
    result_files = [f for f in files if f['name'] == artefact_file_name]
    # sort result files just in case
    result_files.sort(key=lambda x: int(x['id']), reverse=True)

    temp_file_path = None
    tags = []
    if result_files:
        temp_dir = tempfile.mkdtemp()
        temp_file_path = client.files.download(result_files[0]['id'], temp_dir)
        tags = result_files[0]['tags']

    return temp_file_path, tags
