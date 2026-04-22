from __future__ import annotations

from wsgiref.simple_server import make_server

from ais_app import create_app
from ais_app.config import CONFIG


def main() -> None:
    app = create_app()
    with make_server(CONFIG.host, CONFIG.port, app) as server:
        print(f"АИС запущена: http://{CONFIG.host}:{CONFIG.port}")
        print(f"PostgreSQL: {CONFIG.postgres_dsn}")
        print("Для остановки нажмите Ctrl+C")
        server.serve_forever()


if __name__ == "__main__":
    main()
