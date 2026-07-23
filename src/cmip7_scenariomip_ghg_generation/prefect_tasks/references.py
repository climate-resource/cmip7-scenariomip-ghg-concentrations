"""
Reference handling
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from attrs import asdict, define
from prefect import task
from prefect.cache_policies import INPUTS, TASK_SOURCE


@define
class ReferenceInfo:
    """Information about a reference"""

    short_name: str
    """Short name of the reference"""

    licence: str
    """Licence applied to the reference's data"""

    reference: str
    """Reference (long text)"""

    resource_type: str
    """Resource type of the reference (used for cross-linking on Zenodo)"""

    url: str
    """URL"""

    doi: str | None = None
    """DOI"""


def ensure_data_references_table_exists(db_cursor: sqlite3.Connection) -> None:
    """
    Ensure that the data references table exists in the database

    Parameters
    ----------
    db_cursor
        Database cursor to use for executing SQL commands
    """
    references_table_check = db_cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table'
        AND name='data_references';
    """).fetchall()

    if not references_table_check:
        # Create the table
        db_cursor.execute("""
            CREATE TABLE data_references(
                short_name VARCHAR(255) NOT NULL PRIMARY KEY,
                licence VARCHAR(255) NOT NULL,
                reference VARCHAR(4095) NOT NULL,
                resource_type VARCHAR(255) NOT NULL,
                url VARCHAR(255) NOT NULL,
                doi VARCHAR(255) NULL
            );
        """)


def save_reference_info_to_db(
    db: Path,
    reference_info: ReferenceInfo | tuple[ReferenceInfo, ...],
) -> list[str]:
    """
    Save reference information to the database

    Parameters
    ----------
    db
        Path to the database (connection is managed by this function for convenience)

    reference_info
        Reference information

    Returns
    -------
    :
        Short names of the references
    """
    db.parent.mkdir(exist_ok=True, parents=True)
    db_connection = sqlite3.connect(db)
    db_connection.row_factory = sqlite3.Row

    if isinstance(reference_info, ReferenceInfo):
        reference_info_for_db = (asdict(reference_info),)
    else:
        reference_info_for_db = tuple(asdict(v) for v in reference_info)  # type: ignore # mypy being stupid

    with db_connection as db_cursor:
        ensure_data_references_table_exists(db_cursor)

        for ri in reference_info_for_db:
            existing = db_cursor.execute(
                "SELECT * FROM data_references WHERE short_name = ?", (ri["short_name"],)
            ).fetchall()

            if not existing:
                # Not in DB, insert
                db_cursor.execute(
                    """
                        INSERT
                        INTO data_references
                        VALUES(:short_name, :licence, :reference, :resource_type, :url, :doi)
                    """,
                    ri,
                )

            elif dict(existing[0]) == ri:
                # All matches, do nothing
                pass

            elif len(existing) > 1:
                # Should be impossible to get here because short_name is unique
                raise NotImplementedError

            else:
                msg = f"Entry is already in the database, but with a different value. {dict(existing[0])=}. {ri=}"
                raise ValueError(msg)

    db_connection.close()

    ret = [ri["short_name"] for ri in reference_info_for_db]

    return ret


@task(
    cache_policy=INPUTS + TASK_SOURCE,
    # If you're running on a machine where you've already run,
    # you may have to turn this on to avoid prefect skipping the task
    # (and no database existing).
    # TODO: when we clean up, caching is the biggest pain point.
    # Somehow figure out how to test and make it behave.
    # refresh_cache=True,
)
def save_references_info_to_db(
    references_info: list[ReferenceInfo],
    db: Path,
) -> list[str]:
    """
    Save reference information to the reference database

    Parameters
    ----------
    references_info
        References information to save

    db
        Database in which to save the information

    Returns
    -------
    :
        Short names of the saved references
    """
    short_names = save_reference_info_to_db(db=db, reference_info=references_info)

    return short_names
