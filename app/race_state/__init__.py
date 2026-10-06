"""Race State Engine: reduces replayed timeline events into the current race state.

``models`` and ``reducer`` are pure (no Redis, no SQLAlchemy). ``repository`` is the
Redis hot store, ``snapshots`` the PostgreSQL snapshot store, ``processor`` the
stream handler, ``service`` the read-side query service used by the API.
"""
