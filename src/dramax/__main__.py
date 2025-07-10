import argparse

from dramatiq.cli import main as dramatiq_cli
from dramatiq.cli import make_argument_parser

from dramax.api.app import run_server


def get_parser() -> argparse.ArgumentParser:
    """
    Get the parser for the drama CLI.
    """
    parser = argparse.ArgumentParser(prog="dramax")
    subparsers = parser.add_subparsers(dest="command", help="dramaX sub-commands")
    subparsers.required = True
    subparsers.add_parser(
        "worker",
        help="Spawn multiple concurrent workers to process tasks",
    )
    subparsers.add_parser("server", help="Deploy server to serve API requests")
    return parser


def cli() -> None:
    """
    Main CLI entrypoint, parses arguments and calls the appropriate sub-command.
    """
    args, _ = get_parser().parse_known_args()
    if args.command == "worker":
        dramatiq_ns, _ = make_argument_parser().parse_known_args()
        dramatiq_ns.broker = "dramax.worker.scheduler"

        dramatiq_cli(dramatiq_ns)
    elif args.command == "server":
        run_server()


if __name__ == "__main__":
    cli()
