"""Legacy implementation adapters. Constructed by ``archive_cli.engine_factory``.

These modules may wrap existing warehouse/serving/provider objects. They must
not import CLI commands or MCP transport, and they must not expose private
database connections on the public adapter surface.
"""
