"""Helper to login to CASDA and save credentials"""

from __future__ import annotations

from astroquery.casda import CasdaClass, Casda
import os

import argparse

def login(
    username: str | None = None,
    store_password: bool = False,
    reenter_password: bool = False,
) -> CasdaClass:
    """Login to CASDA.

    Args:
        username (str | None, optional): CASDA username. Defaults to None.
        store_password (bool, optional): Stores the password securely in your keyring. Defaults to False.
        reenter_password (bool, optional): Asks for the password even if it is already stored in the keyring. This is the way to overwrite an already stored passwork on the keyring. Defaults to False.

    Returns:
        CasdaClass: CASDA class
    """
    casda: CasdaClass = Casda()
    if username is None:
        username = os.environ.get("CASDA_USERNAME")
    if username is None:
        username = input("Please enter your CASDA username: ")

    casda.login(
        username=username,
        store_password=store_password,
        reenter_password=reenter_password,
    )

    return casda

def main() -> None:
    parser = argparse.ArgumentParser(description="Login to CASDA and save credentials")
    parser.add_argument("username", help="Username for CASDA")

    args = parser.parse_args()

    _ = login(username=args.username, store_password=True, reenter_password=True)


if __name__ == "__main__":
    main()
