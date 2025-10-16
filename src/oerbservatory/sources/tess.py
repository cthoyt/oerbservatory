"""Ingest TeSS."""

from collections import Counter

import click
import pyobo
import rdflib
import ssslm
from dalia_dif.namespace import BIBO, HCRT, MODALIA, SPDX_LICENSE
from rdflib import SDO, Namespace, URIRef
from tabulate import tabulate
from tess_downloader import INSTANCES, TeSSClient
from tqdm import tqdm

from dalia_ingest.model import (
    Author,
    EducationalResource,
    Organization,
    resolve_authors,
    write_resources_jsonl,
)
from dalia_ingest.utils import DALIA_MODULE

__all__ = [
    "get_elixir",
    "get_scilifelab",
    "get_taxila",
    "get_tess",
]

RESOURCE_TYPE_MAP: dict[str, URIRef | None] = {
    "video": SDO.VideoObject,
    "series of videos": SDO.VideoObject,
    "youtube video": SDO.VideoObject,
    #
    "computer software": SDO.SoftwareSourceCode,
    "coding": SDO.SoftwareSourceCode,
    "scripts": SDO.SoftwareSourceCode,
    "programming": SDO.SoftwareSourceCode,
    #
    "poster": SDO.Poster,
    #
    "workshop": MODALIA["Workshop"],
    "podcast": SDO.PodcastEpisode,
    #
    "slidedeck": HCRT["slide"],
    "slideshow": HCRT["slide"],
    "slides": HCRT["slide"],
    "slide deck / presentation": HCRT["slide"],
    "slides / presentation": HCRT["slide"],
    "slideck/ presentation": HCRT["slide"],
    "presentation": HCRT["slide"],
    #
    "jupyter notebooks": MODALIA["CodeNotebook"],
    "jupyter notebook": MODALIA["CodeNotebook"],
    #
    "blog post": None,
    #
    "training materials": None,
    "examples": None,
    "documentation": None,
    "bioinformatics": None,
    "hands-on tutorial": None,
    "learning pathway": None,
    "tutorials": None,
    "handbook": None,
    "case studies": None,
    "implementation guidelines": None,
    "additional reading": None,
    "didactic activities": None,
    "mock data": None,
    "how-to guide": None,
    "online course": None,
    "online material": None,
    "education": None,
    "open educational resource": None,
    "tool": None,
    "toolkit": None,
    "e-learning + workshop": None,
    "pdf": None,
    "recording": None,
    "r shiny application": None,
    "free online course": None,
    "carpentries style curriculum": None,
    "training materials with mock data": None,
    "online modules": None,
    "hackathon": None,
    "vignette": None,
    "api reference": None,
    "educational materials": None,
    "exercise": None,
    "handout": None,
    "workflow": None,
    "installation instructions": None,
    "manual": None,
    "talk": None,
    "knowledgebase": None,
    "book": BIBO["book"],
    "notes": None,
    # Topics
    "computational biology": None,
    "computer science": None,
    "data science": None,
    "transcriptomics": None,
    "machine learning": None,
    # Databases
    "life sciences literature database": None,
    "life science literature database": None,
    "viralzone": None,
}
DIFFICULTY_LEVEL_MAP: dict[str, URIRef | None] = {
    "advanced": MODALIA["Expert"],
    "beginner": MODALIA["Beginner"],
    "intermediate": MODALIA["Competent"],
    "notspecified": None,
}
LICENSES: dict[str, URIRef] = {
    "CC-BY-1.0".lower(): SPDX_LICENSE["CC-BY-1.0"],
    "CC-BY-2.0".lower(): SPDX_LICENSE["CC-BY-2.0"],
    "CC-BY-3.0".lower(): SPDX_LICENSE["CC-BY-3.0"],
    "CC-BY-4.0".lower(): SPDX_LICENSE["CC-BY-4.0"],
    "CC-BY-SA-4.0".lower(): SPDX_LICENSE["CC-BY-SA-4.0"],
    "CC-BY-ND-4.0".lower(): SPDX_LICENSE["CC-BY-ND-4.0"],
    "CC-BY-ND-2.0".lower(): SPDX_LICENSE["CC-BY-ND-2.0"],
    "CC-BY-NC-4.0".lower(): SPDX_LICENSE["CC-BY-NC-4.0"],
    "CC-BY-NC-2.0".lower(): SPDX_LICENSE["CC-BY-NC-2.0"],
    "CC-BY-NC-SA-3.0".lower(): SPDX_LICENSE["CC-BY-NC-SA-3.0"],
    "CC-BY-NC-SA-4.0".lower(): SPDX_LICENSE["CC-BY-NC-SA-4.0"],
    "CC-BY-NC-ND-3.0".lower(): SPDX_LICENSE["CC-BY-NC-ND-3.0"],
    "CC-BY-NC-ND-4.0".lower(): SPDX_LICENSE["CC-BY-NC-ND-4.0"],
    "CC0-1.0".lower(): SPDX_LICENSE["CC0-1.0"],
    "MIT".lower(): SPDX_LICENSE["MIT"],
    "gpl-2.0".lower(): SPDX_LICENSE["GPL-2.0"],
    "gpl-3.0-only".lower(): SPDX_LICENSE["GPL-3.0-only"],
    "gpl-3.0".lower(): SPDX_LICENSE["GPL-3.0-or-later"],
    "agpl-3.0-only".lower(): SPDX_LICENSE["AGPL-3.0-only"],
    "unlicense".lower(): SPDX_LICENSE["Unlicense"],
    "Apache-2.0".lower(): SPDX_LICENSE["Apache-2.0"],
    "BSD-3-Clause".lower(): SPDX_LICENSE["BSD-3-Clause"],
    "Artistic-2.0".lower(): SPDX_LICENSE["Artistic-2.0"],
    "AFL-3.0".lower(): SPDX_LICENSE["AFL-3.0"],
    "ODC-By-1.0".lower(): SPDX_LICENSE["ODC-By-1.0"],
    "WTFPL".lower(): SPDX_LICENSE["WTFPL"],
}

unknown_resource_type: Counter[str] = Counter()


def _get_resource_types(attributes) -> list[URIRef]:
    rv = []
    for resource_type in attributes.get("resource-type", []):
        resource_type = resource_type.lower().strip()
        nn = RESOURCE_TYPE_MAP.get(resource_type)
        if nn:
            rv.append(nn)
        else:
            if not unknown_resource_type[resource_type]:
                tqdm.write(click.style(f'"{resource_type}": None,', fg="red"))
            unknown_resource_type[resource_type] += 1
    return rv


unknown_difficulty = set()


def _get_difficulty_levels(attributes) -> list[URIRef]:
    difficulty_level = attributes.get("difficulty-level")
    if not difficulty_level:
        return []
    difficulty_level = difficulty_level.lower().strip()
    if difficulty_level == "notspecified":
        return []

    xx = DIFFICULTY_LEVEL_MAP.get(difficulty_level)
    if xx:
        return [xx]
    if difficulty_level not in unknown_difficulty:
        unknown_difficulty.add(difficulty_level)
        tqdm.write(click.style(f"unmappable difficulty-level: {difficulty_level}", fg="red"))
    return []


def _get_authors(attributes, ror_grounder: ssslm.Grounder) -> list[Author | Organization]:
    return resolve_authors(attributes.get("authors", []), ror_grounder=ror_grounder)


def get_tess_oers(
    client: TeSSClient,
    *,
    include_description: bool = True,
    ror_grounder: ssslm.Grounder | None = None,
) -> list[EducationalResource]:
    """Get a TeSS graph."""
    rv = []
    if ror_grounder is None:
        ror_grounder = pyobo.get_grounder("ror")
    for material in client.get_materials():
        attributes = material["attributes"]
        doi = attributes["doi"]
        if doi:
            if not doi.strip() or " " in doi:
                doi = None
            else:
                doi = doi.removeprefix("https://doi.org/").removeprefix("http://doi.org/").strip()
                doi = f"https://doi.org/{doi}"
        r = EducationalResource(
            platform=client.key,
            derived_from=client.base_url + material["links"]["self"],
            external_uri=doi,
            title={"en": attributes["title"].strip()},
            license=_get_license(attributes),
            description={"en": attributes["description"].strip()},
            keywords=[{"en": kw.strip()} for kw in attributes.get("keywords") or []],
            date_published=attributes.get("date-published"),
            resource_types=_get_resource_types(attributes),
            difficulty_level=_get_difficulty_levels(attributes),
            authors=_get_authors(attributes, ror_grounder=ror_grounder),
        )
        rv.append(r)

    tqdm.write(f"[{client.key}] created {len(rv):,} records")
    return rv


def get_elixir() -> list[EducationalResource]:
    """Get processed OERs from the ELIXIR flagship instance of TeSS."""
    client = TeSSClient(key="tess")
    return get_tess_oers(client)


def get_taxila() -> list[EducationalResource]:
    """Get processed OERs from Taxila."""
    client = TeSSClient(key="taxila")
    return get_tess_oers(client)


def get_scilifelab() -> list[EducationalResource]:
    """Get processed OERs from SciLifeLab."""
    client = TeSSClient(key="scilifelab")
    return get_tess_oers(client)


def get_tess(
    *,
    include_description: bool = True,
    ror_grounder: ssslm.Grounder | None = None,
) -> list[EducationalResource]:
    """Get processed OERs from all known TeSS instances."""
    ror_grounder = pyobo.get_grounder("ror")
    resources = []
    for key in tqdm(INSTANCES):
        client = TeSSClient(key=key)
        resources.extend(
            get_tess_oers(
                client=client, include_description=include_description, ror_grounder=ror_grounder
            )
        )
    return resources


PANTRAINING = ("pantraining", "https://pan-training.eu")

SPDX_GROUNDER = pyobo.get_grounder("spdx")
license_counter: Counter[str] = Counter()


def _get_license(attributes) -> URIRef | str | None:
    license_text = attributes["licence"].lower().strip()
    if license_text is None or license_text == "notspecified":
        return None

    if license_text in LICENSES:
        return LICENSES[license_text]

    if license_text not in license_counter:
        tqdm.write(f"Missing license for {license_text}")

    license_counter[license_text] += 1

    # TODO upgrade to URI
    return license_text


@click.command()
@click.option(
    "--include-description",
    is_flag=True,
    help="Explicitly include the description, which creates a lot of visual noise in TTL, "
    "so it's left out during development",
)
def main(include_description: bool) -> None:
    """Convert TeSS to DALIA."""
    ror_grounder = pyobo.get_grounder("ror")

    for key in tqdm(INSTANCES):
        client = TeSSClient(key=key)
        resources = get_tess_oers(
            client=client, include_description=include_description, ror_grounder=ror_grounder
        )
        write_resources_jsonl(resources, DALIA_MODULE.join(name=f"{key}.json"))

    click.echo(tabulate(unknown_resource_type.most_common()))
    click.echo("")
    click.echo(tabulate(license_counter.most_common()))


if __name__ == "__main__":
    main()
