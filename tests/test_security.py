from ingest import security


def test_split_statements_drops_comments_and_blanks():
    sql = """
    -- header comment
    use schema HEALTHCARE.SECURITY;   -- trailing comment
    create masking policy if not exists p as (val string) returns string -> '***MASKED***';

    ;
    """
    assert security.split_statements(sql) == [
        "use schema HEALTHCARE.SECURITY",
        "create masking policy if not exists p as (val string) returns string -> '***MASKED***'",
    ]


class FakeCursor:
    def __init__(self, log):
        self.log = log

    def execute(self, sql):
        self.log.append(sql)

    def close(self):
        pass


class FakeConn:
    def __init__(self):
        self.log = []

    def cursor(self):
        return FakeCursor(self.log)


def test_apply_security_runs_policies_file(tmp_path):
    (tmp_path / "policies.sql").write_text("select 1;\nselect 2;\n")
    conn = FakeConn()
    assert security.apply_security(conn, tmp_path) == 2
    assert conn.log == ["select 1", "select 2"]
