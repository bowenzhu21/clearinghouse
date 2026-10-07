import argparse
import json
import os
from pathlib import Path
import sys
import time

from .api import create_server
from .benchmark import benchmark
from .demo import run_demo
from .ledger import Inbox, Ledger
from .models import ConflictError, NotFoundError, ValidationError
from .reconcile import reconcile


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Durable payment events, balanced journals, and settlement reconciliation")
    sub = parser.add_subparsers(dest="command", required=True)
    demo = sub.add_parser("demo", help="execute synthetic failure scenarios and write an HTML report")
    demo.add_argument("--output", default="demo-output")
    demo.add_argument("--operations", type=int, default=200)
    serve = sub.add_parser("serve", help="run the local HTTP API")
    serve.add_argument("--db", default="data/ledger.db")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    worker = sub.add_parser("worker", help="deliver outbox jobs to the local example consumer inbox")
    worker.add_argument("--db", default="data/ledger.db")
    worker.add_argument("--inbox", default="data/inbox.db")
    worker.add_argument("--once", action="store_true", help="drain jobs available now, then exit")
    rec = sub.add_parser("reconcile", help="compare a provider CSV with posted provider events")
    rec.add_argument("csv", type=Path)
    rec.add_argument("--db", default="data/ledger.db")
    rec.add_argument("--output", type=Path)
    bench = sub.add_parser("bench", help="measure sequential local commits")
    bench.add_argument("--operations", type=int, default=1000)
    bench.add_argument("--output", type=Path)
    replay = sub.add_parser("replay", help="requeue a dead-letter job with an audit reason")
    replay.add_argument("job_id")
    replay.add_argument("--reason", required=True)
    replay.add_argument("--db", default="data/ledger.db")
    args = parser.parse_args(argv)
    try:
        if args.command == "demo":
            report = run_demo(args.output, args.operations)
            print(f"Passed {len(report['checks'])} executed scenarios; {len(report['ledger']['journals'])} balanced journals; {report['consumer_receipts']} unique receipts.")
            print(f"Report: {Path(args.output) / 'report.html'}")
        elif args.command == "serve":
            server = create_server(Ledger(args.db), args.host, args.port, os.getenv("CLEARINGHOUSE_API_TOKEN"))
            print(f"Clearinghouse API listening at http://{args.host}:{server.server_port}", flush=True)
            try:
                server.serve_forever()
            finally:
                server.server_close()
        elif args.command == "worker":
            ledger, inbox = Ledger(args.db), Inbox(args.inbox)
            delivered = duplicates = 0
            while True:
                job = ledger.claim()
                if job is None:
                    if args.once:
                        break
                    time.sleep(0.5)
                    continue
                try:
                    inserted = inbox.deliver(job)
                    if ledger.acknowledge(job["job_id"], job["lease_token"]):
                        delivered += int(inserted)
                        duplicates += int(not inserted)
                except Exception as exc:
                    ledger.fail(job["job_id"], job["lease_token"], str(exc))
            print(json.dumps({"delivered": delivered, "duplicates_suppressed": duplicates}))
        elif args.command == "reconcile":
            result = reconcile(args.csv.read_text(), Ledger(args.db).expected_settlements())
            _output(result, args.output)
        elif args.command == "bench":
            _output(benchmark(args.operations), args.output)
        elif args.command == "replay":
            Ledger(args.db).replay_dead_letter(args.job_id, args.reason)
            print(json.dumps({"requeued": args.job_id}))
    except KeyboardInterrupt:
        return 0
    except (ValidationError, ConflictError, NotFoundError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


def _output(value: object, destination: Path | None) -> None:
    text = json.dumps(value, indent=2) + "\n"
    if destination:
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(text)
    else:
        print(text, end="")
