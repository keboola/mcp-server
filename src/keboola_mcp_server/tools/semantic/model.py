"""Shared semantic tool models."""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

# Shared input models


class SemanticObjectType(str, Enum):
    SEMANTIC_MODEL = 'semantic-model'
    SEMANTIC_DATASET = 'semantic-dataset'
    SEMANTIC_METRIC = 'semantic-metric'
    SEMANTIC_RELATIONSHIP = 'semantic-relationship'
    SEMANTIC_GLOSSARY = 'semantic-glossary'
    SEMANTIC_CONSTRAINT = 'semantic-constraint'


class SemanticObjectTypeSelection(BaseModel):
    """Semantic object type selection used by semantic tools."""

    object_type: SemanticObjectType = Field(description='Semantic object type to load.')
    ids: tuple[str, ...] = Field(
        default=tuple(),
        description='Specific object UUIDs to include. Empty list [] means include all objects of this type.',
    )


class SemanticObjectRef(BaseModel):
    """Typed semantic object reference."""

    object_type: SemanticObjectType = Field(description='Semantic object type.')
    id: str = Field(description='Semantic object UUID.')


class SemanticSchemaDefinition(BaseModel):
    """Semantic schema definition returned by the semantic schema tool."""

    model_config = ConfigDict(populate_by_name=True)

    semantic_type: SemanticObjectType = Field(description='Semantic object type.')
    schema_definition: dict[str, Any] = Field(
        validation_alias='schema',
        serialization_alias='schema',
        description='JSON schema for the semantic object type.',
    )
