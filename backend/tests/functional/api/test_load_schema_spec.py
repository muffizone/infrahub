from __future__ import annotations
from typing import TYPE_CHECKING, Any
from tests.helpers.test_app import TestInfrahubApp

if TYPE_CHECKING:
    from infrahub_sdk import InfrahubClient

class TestLoadSchemaSpec(TestInfrahubApp):

    async def test_load_schema_spec(
        self,
        client: InfrahubClient,
    ) -> None:

        schema: dict[str, Any] = {
            "version": "1.0",
            "nodes": [
                {
                    "name": "Car",
                    "namespace": "Test",
                    "default_filter": "name__value",
                    "attributes": [
                        {"name": "name",
                         "kind": "Text",
                         "spec": {"max_length": 7}},
                    ],
                }
            ]
        }

        res = await client.schema.load(schemas=[schema])
        assert len(res.errors) == 0, res.errors

        car_1 = await client.create(kind="TestCar", spec={"name": "car_1"})
        await car_1.save()
