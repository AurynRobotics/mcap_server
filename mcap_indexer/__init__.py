"""mcap_indexer — local-filesystem MCAP catalog daemon.

Watches a folder of `.mcap` recordings and keeps the SQLite catalog
(see ``schema.sql``) in sync: insert/update on add or modify, hard-delete
on remove. The daemon is the single writer to the database.
"""

__version__ = "0.1.0"
