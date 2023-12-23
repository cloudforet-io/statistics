from spaceone.core.pygrpc.server import GRPCServer
from .resource import Resource
# from .schedule import Schedule
# from .history import History


_all_ = ['app']

app = GRPCServer()
app.add_service(Resource)
# app.add_service(Schedule)
# app.add_service(History)
