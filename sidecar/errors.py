"""Exceptions raised by the operator sidecar."""


class SidecarConfigError(Exception):
    """
    Raised when the sidecar's environment-variable configuration is missing
    or invalid.
    """

    def __init__(self, detail: str):
        self.message = f"Invalid sidecar configuration: {detail}"
        super().__init__(self.message)


class ConsulRegistrationError(Exception):
    """
    Raised when registering or deregistering the sidecar's own service
    instance with Consul fails.
    """

    def __init__(self, service_id: str, detail: str = ""):
        self.service_id = service_id
        self.message = (
            f"Failed to update Consul registration for '{service_id}': {detail}"
            if detail
            else f"Failed to update Consul registration for '{service_id}'"
        )
        super().__init__(self.message)
