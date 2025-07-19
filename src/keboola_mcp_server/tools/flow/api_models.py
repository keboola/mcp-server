"""
Raw API Models for Flow endpoints - Pure data transfer objects that mirror API responses exactly.

These models represent the raw data returned by Keboola Flow APIs.
They contain no business logic and use the exact field names and structures from the APIs.
"""

from typing import Any, Optional

from pydantic import AliasChoices, BaseModel, Field, model_validator

from keboola_mcp_server.client import ORCHESTRATOR_COMPONENT_ID


class APIFlowResponse(BaseModel):
    """
    Raw API response for flow configuration endpoints.

    Mirrors the actual JSON structure returned by Keboola Storage API for:
    - flow_detail()
    - flow_list()
    - flow_create()
    - flow_update()
    """

    # Core identification fields
    component_id: str = Field(
        description='The ID of the component (always orchestrator for flows)',
        validation_alias=AliasChoices('component_id', 'componentId', 'component-id'),
        serialization_alias='componentId',
    )
    configuration_id: str = Field(
        description='The ID of the flow configuration',
        validation_alias=AliasChoices('id', 'configuration_id', 'configurationId', 'configuration-id'),
        serialization_alias='id',
    )
    name: str = Field(description='The name of the flow configuration')
    description: Optional[str] = Field(default=None, description='The description of the flow configuration')

    # Versioning and state
    version: int = Field(description='The version of the flow configuration')
    is_disabled: bool = Field(
        default=False,
        description='Whether the flow configuration is disabled',
        validation_alias=AliasChoices('isDisabled', 'is_disabled', 'is-disabled'),
        serialization_alias='isDisabled',
    )
    is_deleted: bool = Field(
        default=False,
        description='Whether the flow configuration is deleted',
        validation_alias=AliasChoices('isDeleted', 'is_deleted', 'is-deleted'),
        serialization_alias='isDeleted',
    )

    # Flow-specific configuration data (as returned by API)
    configuration: dict[str, Any] = Field(
        description='The nested flow configuration object containing phases and tasks'
    )

    # Change tracking
    change_description: Optional[str] = Field(
        default=None,
        description='The description of the latest changes',
        validation_alias=AliasChoices('changeDescription', 'change_description', 'change-description'),
        serialization_alias='changeDescription',
    )

    # Metadata
    metadata: list[dict[str, Any]] = Field(
        default_factory=list,
        description='Flow configuration metadata',
        validation_alias=AliasChoices('metadata', 'configuration_metadata', 'configurationMetadata'),
    )

    # Timestamps
    created: Optional[str] = Field(None, description='Creation timestamp')
    updated: Optional[str] = Field(None, description='Last update timestamp')

    @model_validator(mode='before')
    @classmethod
    def _initialize_component_id_to_orchestrator(cls, data: Any) -> Any:
        """Initialize component_id to Orchestrator if not provided."""
        if isinstance(data, dict) and 'component_id' not in data:
            data['component_id'] = ORCHESTRATOR_COMPONENT_ID
        return data
