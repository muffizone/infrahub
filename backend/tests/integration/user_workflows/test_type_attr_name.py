from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pytest

from infrahub.core.constants.infrahubkind import STANDARDGROUP
from infrahub.core.protocols import CoreStandardGroup
from infrahub.core.schema import SchemaRoot
from infrahub.services import services
from infrahub_sdk.exceptions import GraphQLError

from infrahub.core import registry
from infrahub.core.constants import BranchConflictKeep, DiffAction, InfrahubKind, ProposedChangeState
from infrahub.core.constants.database import DatabaseEdgeType
from infrahub.core.diff.model.path import BranchTrackingId, ConflictSelection, EnrichedDiffRoot
from infrahub.core.diff.repository.repository import DiffRepository
from infrahub.core.initialization import create_branch
from infrahub.core.manager import NodeManager
from infrahub.core.node import Node
from infrahub.core.timestamp import Timestamp
from infrahub.dependencies.registry import get_component_registry
from infrahub.services.adapters.cache.redis import RedisCache
from tests.constants import TestKind
from tests.helpers.graphql import graphql_query
from tests.helpers.schema import CAR_SCHEMA, load_schema
from tests.helpers.test_app import TestInfrahubApp

if TYPE_CHECKING:
    from infrahub_sdk import InfrahubClient

    from infrahub.core.branch import Branch
    from infrahub.database import InfrahubDatabase
    from tests.adapters.message_bus import BusSimulator



class TestDiffUpdateConflict(TestInfrahubApp):

    async def test_type_name_attr(
        self,
        db: InfrahubDatabase,
        default_branch,
        client: InfrahubClient,
    ):
        schema = {
            "version": "1.0",
            "nodes": [
                {
                    "name": "Node",
                    "namespace": "Infra",
                    "display_labels": [
                        "namespace__value"
                    ],
                    "attributes": [
                        {
                            "name": "namespace",
                            "kind": "Text",
                            "optional": False
                        }
                    ]
                }
            ]
        }

        await load_schema(db, schema=SchemaRoot(**schema))
        node = await Node.init(schema="InfraNode", db=db)
        await node.new(db=db, namespace="test_type")
        await node.save(db=db)

        print(f"{node.id=}")

        group = await Node.init(schema=STANDARDGROUP, db=db)
        await group.new(db=db, name="test_group", members=[node])
        await group.save(db=db)

        print(f"{group.id=}")

        query = """
            query {
              CoreStandardGroup {
                edges {
                  node {
                    members {
                      edges {
                        node {
                          display_label
                        }
                      }
                    }
                  }
                }
              }
            }
        """

        result = await graphql_query(query=query, db=db, service=services.service, branch=default_branch)
        print(f"{result.data=}")
        print(f"{result.errors=}")
