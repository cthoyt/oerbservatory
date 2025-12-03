"""CLI for OER sources."""

from collections.abc import Callable

import click
from tqdm import tqdm

from oerbservatory.model import (
    EducationalResource,
    write_resources_jsonl,
    write_resources_sentence_transformer,
    write_resources_tfidf,
    write_sqlite_fti,
)
from oerbservatory.sources.utils import OUTPUT_DIR

__all__ = ["main"]


@click.command()
def main() -> None:
    """Get OER sources."""
    from oerbservatory.sources.dalia import get_dalia
    from oerbservatory.sources.tess import get_all_tess

    functions: list[Callable[[], list[EducationalResource]]] = [
        get_all_tess,
        get_dalia,
        # get_oerhub,
        # get_oersi,
    ]
    resources: list[EducationalResource] = []
    source_iterator = tqdm(functions, desc="OER source", leave=False)
    for f in source_iterator:
        key = f.__name__.removeprefix("get_")
        source_iterator.set_description(key)
        specific = f()
        if not specific:
            tqdm.write(click.style(f"no resources found for {key}", fg="red"))
            continue

        d = OUTPUT_DIR.joinpath(key)
        d.mkdir(exist_ok=True)
        d.joinpath(key)

        write_resources_jsonl(resources, d.joinpath(f"{key}.jsonl"))

        if key == "dalia":
            write_resources_tfidf(
                resources,
                d.joinpath(f"{key}-tfidf-index.tsv"),
                d.joinpath(f"{key}-tfidf-similarities.tsv"),
            )
            write_resources_sentence_transformer(
                resources,
                d.joinpath(f"{key}-transformers-index.tsv"),
                d.joinpath(f"{key}-tranformers-similarities.tsv"),
            )
            write_sqlite_fti(resources, d.joinpath(f"{key}-sqlite-full-text-index.db"))

        resources.extend(specific)

    click.echo(f"got {len(resources):,} resources")


if __name__ == "__main__":
    main()
