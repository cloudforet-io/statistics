from spaceone.core.pygrpc.server import GRPCServer
from .history import History
from .resource import Resource
from .schedule import Schedule

_all_ = ['app']

app = GRPCServer()
app.add_service(History)
app.add_service(Resource)
app.add_service(Schedule)
