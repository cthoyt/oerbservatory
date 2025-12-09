"""Demonstrate converting DALIA DIF v1.3 to TeSS."""

from collections.abc import Callable

import click
import pystow
from dalia_dif.dif13 import EducationalResourceDIF13
from dalia_dif.dif13.rdf import get_discipline_label
from tess_downloader import LearningMaterial, TeSSClient, Topic
from tqdm import tqdm

from oerbservatory.model import EN, EducationalResource
from oerbservatory.sources.dalia import get_dalia
from oerbservatory.sources.gtn import get_gtn
from oerbservatory.sources.oerhub import get_oerhub
from oerbservatory.sources.oersi import get_oersi

__all__ = [
    "export_tess",
]


def export_tess(oer: EducationalResource) -> LearningMaterial | None:
    """Export from an OERbservatory learning material to a TeSS learning material."""
    title = oer.title.get(EN) if oer.title else None
    description = oer.title.get(EN) if oer.description else None
    if not title:
        return None

    return LearningMaterial(
        slug=None,
        title=title,
        url=oer.external_uri,
        description=description,
        keywords=[k[EN] for k in oer.keywords if EN in k] if oer.keywords else None,
        resource_type=None,
        other_types=None,
        scientific_topics=[
            Topic(
                preferred_label=get_discipline_label(discipline),
                uri=str(discipline),
            )
            for discipline in oer.disciplines
        ],
        doi=None,
        license=None,
        contributors=None,
        authors=None,
        status=None,
        version=None,
        external_resources=None,
        difficulty_level="notspecified",
        target_audience=None,
        prerequisites=None,
        fields=None,
        learning_objectives=None,
        date_created=None,
        date_modified=None,
        date_published=None,
        last_scraped=None,
        scraper_record=None,
        created_at=None,
        updated_at=None,
    )


def _from_dalia_dif13(oer: EducationalResourceDIF13) -> LearningMaterial | None:
    if not oer.description:
        return None
    return LearningMaterial(
        slug=str(oer.uuid),  # the slug is the
        title=oer.title,
        url=oer.links[0],
        description=oer.description,
        keywords=oer.keywords,
        # resource_type, # TODO needs the DALIA-TeSS mapping
        # other_types
        scientific_topics=[
            Topic(
                preferred_label=get_discipline_label(discipline),
                uri=str(discipline),
            )
            for discipline in oer.disciplines or []
        ],
    )


@click.command()
@click.option("--test", is_flag=True)
@click.option("--include-oersi", is_flag=True)
def main(test: bool, include_oersi: bool) -> None:
    """Upload content to various mTeSS-X instances."""
    email = pystow.get_config("panosc", "test_email")
    api_key = pystow.get_config("panosc", "test_api_token")
    functions: list[tuple[Callable[[], list[EducationalResource]], str]] = [
        (get_dalia, "dalia"),
        (get_gtn, "kcd"),
        (get_oerhub, "oerhub"),
    ]
    if include_oersi:
        functions.append((get_oersi, "oersi"))
    for func, mtessx_space in functions:
        base_url = "https://test.tesshub.hzdr.de/"
        client = TeSSClient(key="test" if test else mtessx_space, base_url=base_url)
        resources = func()
        for resource in tqdm(resources, desc=mtessx_space):
            tess_resource = export_tess(resource)
            if tess_resource:
                client.post(tess_resource, email=email, api_key=api_key)


if __name__ == "__main__":
    main()
