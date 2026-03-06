from __future__ import annotations

import argparse
import json
from pathlib import Path

from legal_rag import LegalRAG


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Construye y sincroniza índice RAG legal (federal + estatal/CDMX) desde Diputados."
    )
    parser.add_argument(
        "--mode",
        choices=["federal", "state", "all", "updates"],
        default="federal",
        help="federal (default), state, all o updates.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limita leyes federales a indexar (útil para pruebas).",
    )
    parser.add_argument(
        "--state-limit",
        type=int,
        default=None,
        help="Limita la cantidad de estados a indexar (sin contar CDMX).",
    )
    parser.add_argument(
        "--state-max-pages",
        type=int,
        default=30,
        help="Máximo de páginas a rastrear por entrypoint estatal (default 30).",
    )
    parser.add_argument(
        "--no-cdmx",
        action="store_true",
        help="Excluye CDMX en indexación estatal.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.15,
        help="Pausa entre descargas en segundos (default 0.15).",
    )
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent
    rag = LegalRAG(base_dir)
    stats: dict[str, object] = {}

    if args.mode in {"federal", "all"}:
        stats["federal"] = rag.rebuild_from_diputados(limit=args.limit, delay_seconds=args.delay)

    if args.mode in {"state", "all"}:
        stats["state"] = rag.rebuild_state_laws_from_gobiernos(
            limit_states=args.state_limit,
            include_cdmx=not args.no_cdmx,
            per_entry_max_pages=args.state_max_pages,
            delay_seconds=args.delay,
        )

    if args.mode in {"updates", "all"}:
        stats["updates"] = rag.check_federal_updates(refresh_on_change=True)

    status = rag.status()

    print("== RAG build stats ==")
    print(json.dumps(stats, indent=2, ensure_ascii=False))
    print("\n== RAG status ==")
    print(json.dumps(status, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
