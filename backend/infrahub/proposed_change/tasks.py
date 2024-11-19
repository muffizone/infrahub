from __future__ import annotations

from infrahub_sdk.protocols import CoreProposedChange
from prefect import flow, task
from prefect.client.schemas.objects import (
    State,  # noqa: TCH002
)
from prefect.logging import get_run_logger
from prefect.states import Completed, Failed

from infrahub.core import registry
from infrahub.core.branch import Branch
from infrahub.core.branch.tasks import merge_branch
from infrahub.core.constants import InfrahubKind, RepositoryInternalStatus, ValidatorConclusion
from infrahub.core.diff.coordinator import DiffCoordinator
from infrahub.core.protocols import CoreDataCheck, CoreGeneratorDefinition, CoreValidator
from infrahub.core.protocols import CoreProposedChange as InternalCoreProposedChange
from infrahub.dependencies.registry import get_component_registry
from infrahub.generators.models import ProposedChangeGeneratorDefinition
from infrahub.message_bus import InfrahubMessage, messages
from infrahub.message_bus.operations.requests.proposed_change import DefinitionSelect
from infrahub.proposed_change.constants import ProposedChangeState
from infrahub.proposed_change.models import (
    RequestProposedChangeDataIntegrity,
    RequestProposedChangeRepositoryChecks,
    RequestProposedChangeRunGenerators,
)
from infrahub.services import services
from infrahub.workflows.catalogue import REQUEST_PROPOSED_CHANGE_REPOSITORY_CHECKS


async def _proposed_change_transition_state(
    state: ProposedChangeState,
    proposed_change: InternalCoreProposedChange | None = None,
    proposed_change_id: str | None = None,
) -> None:
    service = services.service
    async with service.database.start_session() as db:
        if proposed_change is None and proposed_change_id:
            proposed_change = await registry.manager.get_one(
                db=db, id=proposed_change_id, kind=InternalCoreProposedChange, raise_on_error=True
            )
        if proposed_change:
            proposed_change.state.value = state.value  # type: ignore[misc]
            await proposed_change.save(db=db)


# async def proposed_change_transition_merged(flow: Flow, flow_run: FlowRun, state: State) -> None:
#     await _proposed_change_transition_state(
#         proposed_change_id=flow_run.parameters["proposed_change_id"], state=ProposedChangeState.MERGED
#     )


# async def proposed_change_transition_open(flow: Flow, flow_run: FlowRun, state: State) -> None:
#     await _proposed_change_transition_state(
#         proposed_change_id=flow_run.parameters["proposed_change_id"], state=ProposedChangeState.OPEN
#     )


@flow(
    name="proposed-change-merge",
    flow_run_name="Merge propose change: {proposed_change_name} ",
    description="Merge a given proposed change.",
    # TODO need to investigate why these function are not working as expected
    # on_completion=[proposed_change_transition_merged],  # type: ignore
    # on_failure=[proposed_change_transition_open],  # type: ignore
    # on_crashed=[proposed_change_transition_open],  # type: ignore
    # on_cancellation=[proposed_change_transition_open],  # type: ignore
)
async def merge_proposed_change(proposed_change_id: str, proposed_change_name: str) -> State:  # pylint: disable=unused-argument
    service = services.service
    log = get_run_logger()

    async with service.database.start_session() as db:
        proposed_change = await registry.manager.get_one(
            db=db, id=proposed_change_id, kind=InternalCoreProposedChange, raise_on_error=True
        )

        log.info("Validating if all conditions are met to merge the proposed change")

        source_branch = await Branch.get_by_name(db=db, name=proposed_change.source_branch.value)
        validations = await proposed_change.validations.get_peers(db=db, peer_type=CoreValidator)
        for validation in validations.values():
            validator_kind = validation.get_kind()
            if (
                validator_kind != InfrahubKind.DATAVALIDATOR
                and validation.conclusion.value.value != ValidatorConclusion.SUCCESS.value
            ):
                # Ignoring Data integrity checks as they are handled again later
                await _proposed_change_transition_state(proposed_change=proposed_change, state=ProposedChangeState.OPEN)
                return Failed(message="Unable to merge proposed change containing failing checks")
            if validator_kind == InfrahubKind.DATAVALIDATOR:
                data_checks = await validation.checks.get_peers(db=db, peer_type=CoreDataCheck)
                for check in data_checks.values():
                    if check.conflicts.value and not check.keep_branch.value:
                        await _proposed_change_transition_state(
                            proposed_change=proposed_change, state=ProposedChangeState.OPEN
                        )
                        return Failed(
                            message="Data conflicts found on branch and missing decisions about what branch to keep"
                        )

        log.info("Proposed change is eligible to be merged")
        await merge_branch(branch=source_branch.name)

        log.info(f"Branch {source_branch.name} has been merged successfully")

        await _proposed_change_transition_state(proposed_change=proposed_change, state=ProposedChangeState.MERGED)
        return Completed(message="proposed change merged successfully")


@flow(
    name="proposed-changes-cancel-branch",
    flow_run_name="Cancel all proposed change associated with branch {branch_name}",
    description="Cancel all Proposed change associated with a branch.",
)
async def cancel_proposed_changes_branch(branch_name: str) -> None:
    service = services.service
    proposed_changed_opened = await service.client.filters(
        kind=CoreProposedChange,
        include=["id", "source_branch"],
        state__value=ProposedChangeState.OPEN.value,
        source_branch__value=branch_name,
    )
    proposed_changed_closed = await service.client.filters(
        kind=CoreProposedChange,
        include=["id", "source_branch"],
        state__value=ProposedChangeState.CLOSED.value,
        source_branch__value=branch_name,
    )

    for proposed_change in proposed_changed_opened + proposed_changed_closed:
        await cancel_proposed_change(proposed_change=proposed_change)


@task(name="Cancel a propose change", description="Cancel a propose change")
async def cancel_proposed_change(proposed_change: CoreProposedChange) -> None:
    service = services.service
    log = get_run_logger()

    log.info("Canceling proposed change as the source branch was deleted")
    proposed_change = await service.client.get(kind=CoreProposedChange, id=proposed_change.id)
    proposed_change.state.value = ProposedChangeState.CANCELED.value
    await proposed_change.save()


@flow(
    name="proposed-changed-data-integrity",
    flow_run_name="Triggers data integrity check",
)
async def run_proposed_change_data_integrity_check(model: RequestProposedChangeDataIntegrity) -> None:
    """Triggers a data integrity validation check on the provided proposed change to start."""

    service = services.service
    destination_branch = await registry.get_branch(db=service.database, branch=model.destination_branch)
    source_branch = await registry.get_branch(db=service.database, branch=model.source_branch)
    component_registry = get_component_registry()
    async with service.database.start_transaction() as dbt:
        diff_coordinator = await component_registry.get_component(DiffCoordinator, db=dbt, branch=source_branch)
        await diff_coordinator.update_branch_diff(base_branch=destination_branch, diff_branch=source_branch)


@flow(
    name="proposed-changed-run-generator",
    flow_run_name="Run generators related to proposed change {model.proposed_change}",
)
async def run_generators(model: RequestProposedChangeRunGenerators) -> None:
    service = services.service
    generators = await service.client.filters(
        kind=CoreGeneratorDefinition,
        prefetch_relationships=True,
        populate_store=True,
        branch=model.source_branch,
    )
    generator_definitions = [
        ProposedChangeGeneratorDefinition(
            definition_id=generator.id,
            definition_name=generator.name.value,
            class_name=generator.class_name.value,
            file_path=generator.file_path.value,
            query_name=generator.query.peer.name.value,
            query_models=generator.query.peer.models.value,
            repository_id=generator.repository.peer.id,
            parameters=generator.parameters.value,
            group_id=generator.targets.peer.id,
            convert_query_response=generator.convert_query_response.value,
        )
        for generator in generators
    ]

    for generator_definition in generator_definitions:
        # Request generator definitions if the source branch that is managed in combination
        # to the Git repository containing modifications which could indicate changes to the transforms
        # in code
        # Alternatively if the queries used touches models that have been modified in the path
        # impacted artifact definitions will be included for consideration

        select = DefinitionSelect.NONE
        select = select.add_flag(
            current=select,
            flag=DefinitionSelect.FILE_CHANGES,
            condition=model.source_branch_sync_with_git and model.branch_diff.has_file_modifications,
        )

        for changed_model in model.branch_diff.modified_kinds(branch=model.source_branch):
            select = select.add_flag(
                current=select,
                flag=DefinitionSelect.MODIFIED_KINDS,
                condition=changed_model in generator_definition.query_models,
            )

        if select:
            msg = messages.RequestGeneratorDefinitionCheck(
                generator_definition=generator_definition,
                branch_diff=model.branch_diff,
                proposed_change=model.proposed_change,
                source_branch=model.source_branch,
                source_branch_sync_with_git=model.source_branch_sync_with_git,
                destination_branch=model.destination_branch,
            )
            msg.assign_meta(parent=model)
            await service.send(message=msg)

    next_messages: list[InfrahubMessage] = []
    if model.refresh_artifacts:
        next_messages.append(
            messages.RequestProposedChangeRefreshArtifacts(
                proposed_change=model.proposed_change,
                source_branch=model.source_branch,
                source_branch_sync_with_git=model.source_branch_sync_with_git,
                destination_branch=model.destination_branch,
                branch_diff=model.branch_diff,
            )
        )

    if model.do_repository_checks:
        model_proposed_change_repo_checks = RequestProposedChangeRepositoryChecks(
            proposed_change=model.proposed_change,
            source_branch=model.source_branch,
            source_branch_sync_with_git=model.source_branch_sync_with_git,
            destination_branch=model.destination_branch,
            branch_diff=model.branch_diff,
        )
        await service.workflow.submit_workflow(
            workflow=REQUEST_PROPOSED_CHANGE_REPOSITORY_CHECKS, parameters={"model": model_proposed_change_repo_checks}
        )

    for next_msg in next_messages:
        next_msg.assign_meta(parent=model)
        await service.send(message=next_msg)


@flow(
    name="proposed-changed-repository-checks",
    flow_run_name="Process checks defined in proposed change: {model.proposed_change}",
)
async def repository_checks(model: RequestProposedChangeRepositoryChecks) -> None:
    service = services.service
    events: list[InfrahubMessage] = []
    for repository in model.branch_diff.repositories:
        if (
            model.source_branch_sync_with_git
            and not repository.read_only
            and repository.internal_status == RepositoryInternalStatus.ACTIVE.value
        ):
            events.append(
                messages.RequestRepositoryChecks(
                    proposed_change=model.proposed_change,
                    repository=repository.repository_id,
                    source_branch=model.source_branch,
                    target_branch=model.destination_branch,
                )
            )

        events.append(
            messages.RequestRepositoryUserChecks(
                proposed_change=model.proposed_change,
                repository=repository.repository_id,
                source_branch=model.source_branch,
                source_branch_sync_with_git=model.source_branch_sync_with_git,
                target_branch=model.destination_branch,
                branch_diff=model.branch_diff,
            )
        )
    for event in events:
        event.assign_meta(parent=model)
        await service.send(message=event)
