# Naming Rules

- Names describe ownership and transitions: `ProjectBindingService`, `ConsumerGroupCoordinator`, `WorkerProvisioningService`, `WakeTargetService`, `EntityLockRegistry`.
- Use `*_repository.py` for durable state access, `*_service.py` for use cases, `*_coordinator.py` for cross-service policy, `*_mapper.py` for representation changes, and `*_schemas.py` or `*_storage.py` for validated boundaries.
- Function names state their effect or result: `reconcile_capacity`, `claim_request`, `register_worker`, `prune_terminal_request_history`.
- Boolean names read as questions. Collections are plural. Shared protocol values use enums or narrow literals, not loose strings.
- Avoid vague names such as `handle`, `process`, `manager`, or `data` when the concrete role is known.

