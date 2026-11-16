import io

from rich.console import Console

from de.display import Column, Listing


def render(listing, rows):
    console = Console(file=io.StringIO(), width=120)
    listing.print(console, rows)
    return console.file.getvalue()


def test_values_are_formatted_and_missing_ones_shown_as_missing():
    listing = Listing[dict](
        "Counts",
        [
            Column.text("Name", lambda row: row["name"]),
            Column("Count", lambda row: row.get("count"), "{:,}".format, missing="n/a"),
        ],
    )
    output = render(listing, [{"name": "a", "count": 1000}, {"name": "b"}])
    assert output.startswith("Counts\n")
    assert "1,000" in output
    assert "n/a" in output


def test_sections_separate_rows_with_different_keys():
    listing = Listing[tuple[str, int]](
        "",
        [Column.text("Group", lambda row: row[0]), Column("Value", lambda row: row[1])],
        section=lambda row: row[0],
    )
    output = render(listing, [("a", 1), ("a", 2), ("b", 3), ("b", 4)])
    separators = [line for line in output.splitlines() if line.startswith("├")]
    assert len(separators) == 1
