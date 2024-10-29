import sys

import pandas as pd


ARGS = ("db_instance_class", "database_instance_count", "app_connection_pool_max_size")
AURORA_CONNECTION_LIMITS_URL = "https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraMySQL.Managing.Performance.html"


class UserArgs(object):

    __slots__ = (
        "db_instance_class",
        "database_instance_count",
        "pod_count",
        "app_connection_pool_max_size"
    )

    db_instance_class: str
    database_instance_count: int
    pod_count: int
    app_connection_pool_max_size: int

    def __init__(self, *args):

        self.db_instance_class = args[0]
        self.database_instance_count = args[1]
        self.pod_count = args[2]
        self.app_connection_pool_max_size = args[3]


def _parse_args() -> tuple:
    """
    Parse the command line arguments.

    Returns
    -------
    tuple
        The parsed command line arguments.
        The first element is the db_instance_class.
        The second element is the database_instance_count.
        The third element is the pod_count.
        The fourth element is app_connection_pool_max_size.
    """

    try:
        return (
            sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
        )
    except IndexError:
        print(f"Usage: '{sys.argv[0]} {' '.join(ARGS)}'")
        sys.exit(1)


def df_to_dict(
    df: pd.DataFrame, key_index: int = 0, value_index: int = 1, exclude_index=False
) -> dict:
    """
    Convert a DataFrame to a dictionary.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to convert.
    key_index : int
        The index of the column to use as the key.t
    value_index : int
        The index of the column to use as the value.

    Returns
    -------
    dict
        The DataFrame as a dictionary.
    """

    return {
        row[key_index]: row[value_index] for row in df.itertuples(
            index=exclude_index
        )
    }


def get_connection_limits(url: str = AURORA_CONNECTION_LIMITS_URL) -> pd.DataFrame:
    """
    Get the connection limits for Aurora MySQL from the AWS documentation.

    Returns
    -------
    pd.DataFrame
        The connection limits for Aurora MySQL.
    """

    # Get the tables from the URL
    tables = pd.read_html(url)

    # The connection limits table is the first table on the page
    connection_limits = tables[0]

    return connection_limits


def main() -> None:
    """
    Run our main function.
    """
    user_args = UserArgs(*_parse_args())
    connection_limits = get_connection_limits()
    limit_dict = df_to_dict(connection_limits)
    max_app_connections = (
        user_args.pod_count * user_args.app_connection_pool_max_size
    )
    max_db_connections = (
        limit_dict[user_args.db_instance_class] * user_args.database_instance_count
    )
    print(
        f"You have requested {max_app_connections} application connections on an RDS "
        f"Instance configured for {max_db_connections} connections.\nYou are "
        f"{'good!' if max_app_connections <= max_db_connections else 'over the limit!'}"
    )


if __name__ == "__main__":

    main()
