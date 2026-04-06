"""Unit tests for Column and Index dataclasses."""

from data_assets.core.column import Column, Index, PG_MAX_IDENTIFIER, index_name
from data_assets.core.enums import IndexMethod


class TestIndex:
    def test_frozen(self):
        idx = Index(columns=("a",))
        try:
            idx.unique = True
            assert False, "Should not allow mutation"
        except AttributeError:
            pass

    def test_defaults(self):
        idx = Index(columns=("a",))
        assert idx.unique is False
        assert idx.method == IndexMethod.BTREE
        assert idx.where is None
        assert idx.include is None
        assert idx.name is None

    def test_all_fields(self):
        idx = Index(
            columns=("a", "b"),
            unique=True,
            method=IndexMethod.GIN,
            where="a IS NOT NULL",
            include=("c",),
            name="my_custom_idx",
        )
        assert idx.columns == ("a", "b")
        assert idx.unique is True
        assert idx.method == IndexMethod.GIN
        assert idx.where == "a IS NOT NULL"
        assert idx.include == ("c",)
        assert idx.name == "my_custom_idx"


class TestIndexName:
    def test_basic(self):
        idx = Index(columns=("state",))
        assert index_name("my_table", idx) == "ix_my_table_state"

    def test_composite(self):
        idx = Index(columns=("name", "element", "value"))
        assert index_name("choices", idx) == "ix_choices_name_element_value"

    def test_unique_suffix(self):
        idx = Index(columns=("number",), unique=True)
        assert index_name("incidents", idx) == "ix_incidents_number_unique"

    def test_partial_suffix(self):
        idx = Index(columns=("state",), where="state = 'open'")
        assert index_name("prs", idx) == "ix_prs_state_partial"

    def test_unique_and_partial(self):
        idx = Index(columns=("email",), unique=True, where="active = 'true'")
        assert index_name("users", idx) == "ix_users_email_unique_partial"

    def test_explicit_name_wins(self):
        idx = Index(columns=("a",), name="my_explicit_name")
        assert index_name("any_table", idx) == "my_explicit_name"

    def test_truncation_at_63_chars(self):
        long_table = "a_very_long_table_name_that_goes_on_and_on"
        long_col = "another_extremely_long_column_name"
        idx = Index(columns=(long_col,))
        result = index_name(long_table, idx)
        assert len(result) <= PG_MAX_IDENTIFIER
        # Should end with an 8-char hex hash
        assert result[-9] == "_"
        assert all(c in "0123456789abcdef" for c in result[-8:])

    def test_truncation_is_deterministic(self):
        idx = Index(columns=("x" * 60,))
        name1 = index_name("long_table", idx)
        name2 = index_name("long_table", idx)
        assert name1 == name2

    def test_different_long_names_produce_different_hashes(self):
        idx1 = Index(columns=("x" * 60,))
        idx2 = Index(columns=("y" * 60,))
        name1 = index_name("t", idx1)
        name2 = index_name("t", idx2)
        assert name1 != name2
