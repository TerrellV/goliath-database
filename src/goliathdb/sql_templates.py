### SECURITY PERMISSIONS - GRANTS ###

grant_all_on_all_tables_in_schema = """
    GRANT ALL
    ON ALL TABLES IN SCHEMA %s
    TO %s
"""

grant_all_on_database = """
    GRANT ALL
    ON DATABASE %s
    TO %s
"""

grant_all_on_schema = """
    GRANT ALL
    ON SCHEMA %s
    TO %s
"""

grant_all_on_sequences = """
    GRANT ALL
    ON ALL SEQUENCES IN SCHEMA %s
    TO %s
"""
