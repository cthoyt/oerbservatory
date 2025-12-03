"""Parse DALIA curation sheets."""

import csv
import re
from pathlib import Path

import click
from curies import Reference
from dalia_dif.dif13 import (
    AuthorDIF13,
    EducationalResourceDIF13,
    OrganizationDIF13,
    parse_dif13_row,
)
from pydantic import ByteSize
from tqdm import tqdm

from oerbservatory.model import (
    EN,
    Author,
    EducationalResource,
    Organization,
    write_resources_jsonl,
    write_resources_sentence_transformer,
    write_resources_tfidf,
    write_sqlite_fti,
)
from oerbservatory.sources.utils import OUTPUT_DIR

__all__ = [
    "get_dalia",
]


ORCID_RE = re.compile(r"^\d{4}-\d{4}-\d{4}-\d{3}(\d|X)$")

ORCID_URI_PREFIX = "https://orcid.org/"
ROR_URI_PREFIX = "https://ror.org/"
WIKIDATA_URI_PREFIX = "http://www.wikidata.org/entity/"
RE = re.compile(r"^(?P<name>.*)\s\((?P<relation>S|R|SR|RS)\)$")


def _log(path: Path, line: int, text: str) -> None:
    tqdm.write(f"[{path.name} line:{line}] {text}")


def parse(path: str | Path) -> list[EducationalResource]:
    """Parse DALIA records."""
    path = Path(path).expanduser().resolve()
    with path.open(newline="") as csvfile:
        return [
            oer
            for idx, record in enumerate(csv.DictReader(csvfile), start=2)
            if (oer := _omni_process_row(path, idx, record)) is not None
        ]


def _convert(e: EducationalResourceDIF13) -> EducationalResource | None:
    rv = EducationalResource(
        reference=Reference(prefix="dalia.oer", identifier=str(e.uuid)),
        external_uri=e.links,
        title={EN: e.title},
        description={EN: e.description},
        keywords=[{EN: keyword} for keyword in e.keywords],
        authors=[_process_author(a) for a in e.authors],
        difficulty_level=e.proficiency_levels,
        languages=e.languages,
        license=e.license,
        file_formats=e.file_formats,
        date_published=e.publication_date,
        version=e.version,
        audience=e.target_groups,
        file_size=_process_size(e.file_size),
        resource_types=e.learning_resource_types,
        media_types=e.media_types,
        disciplines=e.disciplines,
    )
    return rv


def _process_size(x: str | None) -> ByteSize | None:
    if x is None:
        return None
    if not x.endswith(" MB"):
        raise ValueError
    return ByteSize(int(float(x.removesuffix(" MB")) * 1_000_000))


def _process_author(e: AuthorDIF13 | OrganizationDIF13) -> Author | Organization:
    match e:
        case AuthorDIF13():
            return Author(name=e.name, orcid=e.orcid)
        case OrganizationDIF13():
            return Organization(name=e.name, ror=e.ror, wikidata=e.wikidata)
        case _:
            raise TypeError


def _omni_process_row(path: Path, idx: int, row: dict[str, str]) -> EducationalResource | None:
    """Convert a row in a DALIA curation file to a resource, or return none if unable."""
    ed13 = parse_dif13_row(path.name, idx, row, future=True)
    if ed13 is None:
        return None
    return _convert(ed13)


def get_dif13_paths() -> list[Path]:
    """Get DALIA curation paths."""
    base = Path("/Users/cthoyt/dev/dalia-curation/curation")
    return list(base.glob("*.tsv"))


def get_dalia() -> list[EducationalResource]:
    """Get processed OERs from DALIA."""
    return [resource for path in get_dif13_paths() for resource in parse(path)]


@click.command()
@click.option("--transformers", is_flag=True)
def main(transformers: bool) -> None:
    """Process DALIA curation sheets."""
    resources = get_dalia()
    dire = OUTPUT_DIR.joinpath("dalia")
    write_resources_jsonl(resources, dire.joinpath("dalia.jsonl"))

    write_sqlite_fti(resources, dire.joinpath("dalia-fts-sqlite.db"))

    if transformers:
        write_resources_tfidf(
            resources,
            dire.joinpath("dalia-tfidf-index.tsv"),
            dire.joinpath("dalia-tfidf-similarities.tsv"),
        )
        write_resources_sentence_transformer(
            resources,
            dire.joinpath("dalia-transformers-index.tsv"),
            dire.joinpath("dalia-transformers-similarities.tsv"),
        )


if __name__ == "__main__":
    main()
