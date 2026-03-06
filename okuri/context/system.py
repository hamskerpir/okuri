from dataclasses import dataclass

from nats.js import JetStreamContext

from okuri.resource.manager import NatsResourceManager


@dataclass
class SystemContext:
    resource_manager: NatsResourceManager
    js: JetStreamContext
