"""
Upload results to LLNL's FTP server
"""

import ftplib
import traceback
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Annotated, Optional

import tqdm.auto
import typer


@contextmanager
def login_to_ftp(ftp_server: str, username: str, password: str, dry_run: bool) -> Iterator[Optional[ftplib.FTP]]:
    """
    Create a connection to an FTP server.

    When the context block is excited, the connection is closed.

    If we are doing a dry run, `None` is returned instead
    to signal that no connection was actually made.
    We do, however, log messages to indicate what would have happened.

    Parameters
    ----------
    ftp_server
        FTP server to login to

    username
        Username

    password
        Password

    dry_run
        Is this a dry run?

        If `True`, we won't actually login to the FTP server.

    Yields
    ------
    :
        Connection to the FTP server.

        If it is a dry run, we simply return `None`.
    """
    if dry_run:
        print(f"Dry run. Would log in to {ftp_server} using {username=}")
        ftp = None

    else:
        ftp = ftplib.FTP(ftp_server, passwd=password, user=username)  # noqa: S321
        print(f"Logged into {ftp_server} using {username=}")

    yield ftp

    if ftp is None:
        if not dry_run:  # pragma: no cover
            raise AssertionError
        print(f"Dry run. Would close connection to {ftp_server}")

    else:
        ftp.quit()
        print(f"Closed connection to {ftp_server}")


def cd_v(dir_to_move_to: str, ftp: ftplib.FTP) -> ftplib.FTP:
    """
    Change directory verbosely

    Parameters
    ----------
    dir_to_move_to
        Directory to move to on the server

    ftp
        FTP connection

    Returns
    -------
    :
        The FTP connection
    """
    ftp.cwd(dir_to_move_to)
    # print(f"Now in {ftp.pwd()} on FTP server")

    return ftp


def mkdir_v(dir_to_make: str, ftp: ftplib.FTP) -> None:
    """
    Make directory verbosely

    Also, don't fail if the directory already exists

    Parameters
    ----------
    dir_to_make
        Directory to make

    ftp
        FTP connection
    """
    try:
        # print(f"Attempting to make {dir_to_make} on {ftp.host=}")
        ftp.mkd(dir_to_make)
        # print(f"Made {dir_to_make} on {ftp.host=}")
    except ftplib.error_perm:
        print(f"{dir_to_make} already exists on {ftp.host=}")


def upload_file(
    file: Path,
    strip_pre_upload: Path,
    ftp_dir_upload_in: str,
    ftp: Optional[ftplib.FTP],
) -> Optional[ftplib.FTP]:
    """
    Upload a file to an FTP server

    Parameters
    ----------
    file
        File to upload.

        The full path of the file relative to `strip_pre_upload` will be uploaded.
        In other words, any directories in `file` will be made on the
        FTP server before uploading.

    strip_pre_upload
        The parts of the path that should be stripped before the file is uploaded.

        For example, if `file` is `/path/to/a/file/somewhere/file.nc`
        and `strip_pre_upload` is `/path/to/a`,
        then we will upload the file to `file/somewhere/file.nc` on the FTP server
        (relative to whatever directory the FTP server is in
        when we enter this function).

    ftp_dir_upload_in
        Directory on the FTP server in which to upload `file`
        (after removing `strip_pre_upload`).

    ftp
        FTP connection to use for the upload.

        If this is `None`, we assume this is a dry run.

    Returns
    -------
    :
        The FTP connection.

        If it is a dry run, this can simply be `None`.
    """
    # print(f"Uploading {file}")
    if ftp is None:
        print(f"Dry run. Would cd on the FTP server to {ftp_dir_upload_in}")

    else:
        cd_v(ftp_dir_upload_in, ftp=ftp)

    filepath_upload = file.relative_to(strip_pre_upload)
    # print(
    #     f"Relative to {ftp_dir_upload_in} on the FTP server, " f"will upload {file} to {filepath_upload}",
    # )

    for parent in list(filepath_upload.parents)[::-1]:
        if parent == Path("."):
            continue

        to_make = parent.parts[-1]

        if ftp is None:
            print("Dry run. " "Would ensure existence of " f"and cd on the FTP server to {to_make}")

        else:
            mkdir_v(to_make, ftp=ftp)
            cd_v(to_make, ftp=ftp)

    if ftp is None:
        print(f"Dry run. Would upload {file}")

        return ftp

    with open(file, "rb") as fh:
        upload_command = f"STOR {file.name}"
        # print(f"Upload command: {upload_command}")

        try:
            # print(f"Initiating upload of {file}")
            ftp.storbinary(upload_command, fh)

            # print(f"Successfully uploaded {file}")
        except ftplib.error_perm:
            print(
                f"{file.name} already exists on the server in {ftp.pwd()}. "
                "Use a different directory on the receiving server "
                "if you really wish to upload again."
            )
            raise

    return ftp


def main(
    upload_root_dir: Annotated[
        Path,
        typer.Argument(
            help="""Root directory in which to look for files to upload.

All `*.nc` files found recursively in this directory are uploaded."""
        ),
    ],
    unique_upload_id_dir: Annotated[
        str,
        typer.Option(
            help="Unique directory for identifying the upload. Make this unique to make life for the LLNL team easier."
        ),
    ],
    email: Annotated[str, typer.Option(help="Your email, used for tracking uploads")] = "ghg-team@climate-resource.com",
    dry_run: Annotated[bool, typer.Option(help="Perform a dry run")] = False,
) -> None:
    """Upload files to the LLNL server"""
    # For this server, it's always this
    FTP_DIR_ROOT = "/incoming"

    with login_to_ftp(
        ftp_server="ftp.llnl.gov",
        # Always anonymous for this upload
        username="anonymous",
        password=email,
        dry_run=dry_run,
    ) as ftp:
        print("Opened FTP connection")
        print()

        if dry_run:
            print(f"Would cd to {FTP_DIR_ROOT} on the server")

        else:
            cd_v(FTP_DIR_ROOT, ftp=ftp)

        if dry_run:
            print(f"Would make {unique_upload_id_dir} on the server")
            print(f"Would cd to {unique_upload_id_dir} on the server")

        else:
            mkdir_v(unique_upload_id_dir, ftp=ftp)
            cd_v(unique_upload_id_dir, ftp=ftp)

        n_errors = 0
        n_total = 0
        # As a note: we could parallelise this.
        # The issue is that the FTP server
        # doesn't like having too many open connections at once.
        # Hence we run serially.
        files_to_upload = list(upload_root_dir.rglob("*.nc"))
        for file in tqdm.auto.tqdm(files_to_upload):
            # file_stats = os.stat(file)
            # file_size_mb = file_stats.st_size / (1024 * 1024)
            # file_size_gb = file_stats.st_size / (1024 * 1024 * 1024)
            #
            # print(f"{file=}")
            # print(f"{file_size_mb=:.3f}")
            # print(f"{file_size_gb=:.3f}")

            try:
                upload_file(
                    file,
                    strip_pre_upload=file.parent,
                    ftp_dir_upload_in=f"{FTP_DIR_ROOT}/{unique_upload_id_dir}",
                    ftp=ftp,
                )
                # print(f"Uploaded {file=}")

            except ftplib.error_perm:
                print(f"Failed to upload {file=}")
                traceback.print_exc()
                n_errors += 1

            n_total += 1
            # print()

    print(f"Finished: {n_errors=}, {n_total=}")


if __name__ == "__main__":
    typer.run(main)
