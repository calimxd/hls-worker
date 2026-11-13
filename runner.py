#!/usr/bin/env python3
"""Ephemeral GitHub Actions runner for hlsync -> temporary host -> cleanup."""

import json
import os
import re
import shutil
import subprocess
import tempfile
import urllib.request
import uuid

REPO = os.environ["GH_REPO"]
TOKEN = os.environ["GH_TOKEN"]
ISSUE = os.environ["ISSUE_NUMBER"]

LITTERBOX = "https://litterbox.catbox.moe/resources/internals/api.php"
LITTERBOX_MAX = 980 * 1024 * 1024  # 980 MB safe ceiling under 1 GB
PROGRESO = re.compile(r"(\d+)/(\d+)\s*\((\d+)%\)")


def update_issue(payload: dict, close: bool = False) -> None:
    body = {"body": json.dumps(payload, ensure_ascii=False)}
    if close:
        body["state"] = "closed"
    req = urllib.request.Request(
        f"https://api.github.com/repos/{REPO}/issues/{ISSUE}",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
            "User-Agent": "hls-worker/1.0",
        },
        method="PATCH",
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        resp.read()


def stream_multipart(url: str, fields: dict[str, str], file_field: str, path: str,
                     extra_headers: dict[str, str] | None = None) -> str:
    boundary = uuid.uuid4().hex
    head = bytearray()
    for k, v in fields.items():
        head += (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="{k}"\r\n\r\n{v}\r\n'
        ).encode("utf-8")
    head += (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="{file_field}"; filename="{os.path.basename(path)}"\r\n'
        f"Content-Type: video/mp4\r\n\r\n"
    ).encode("utf-8")
    tail = f"\r\n--{boundary}--\r\n".encode("utf-8")
    size = len(head) + os.path.getsize(path) + len(tail)

    def gen():
        yield bytes(head)
        with open(path, "rb") as fh:
            while chunk := fh.read(4194304):
                yield chunk
        yield tail

    # urllib requires bytes or an iterable only via http.client; stream with http.client
    import http.client
    from urllib.parse import urlsplit

    parts = urlsplit(url)
    conn = http.client.HTTPSConnection(parts.netloc, timeout=1800)
    headers = {
        "Content-Type": f"multipart/form-data; boundary={boundary}",
        "Content-Length": str(size),
        "User-Agent": "Mozilla/5.0",
    }
    if extra_headers:
        headers.update(extra_headers)
    target = parts.path + (f"?{parts.query}" if parts.query else "")
    conn.putrequest("POST", target)
    for hk, hv in headers.items():
        conn.putheader(hk, hv)
    conn.endheaders()
    for block in gen():
        conn.send(block)
    resp = conn.getresponse()
    raw = resp.read().decode("utf-8", "replace")
    conn.close()
    if resp.status >= 400:
        raise RuntimeError(f"HTTP {resp.status}: {raw[:160]}")
    return raw.strip()


def upload_litterbox(path: str, hours: int = 72) -> dict:
    url = stream_multipart(
        LITTERBOX,
        {"reqtype": "fileupload", "time": f"{hours}h"},
        "fileToUpload",
        path,
    )
    if not url.startswith("http"):
        raise RuntimeError(f"litterbox rechazo: {url[:160]}")
    return {"url": url, "expira": f"{hours}h", "host": "litterbox"}


def upload_gofile(path: str) -> dict:
    srv_raw = urllib.request.urlopen("https://api.gofile.io/servers", timeout=20).read().decode()
    srv_data = json.loads(srv_raw)
    servers = srv_data.get("data", {}).get("servers", [])
    if not servers:
        raise RuntimeError("sin servidores gofile disponibles")
    server = servers[0]["name"]
    raw = stream_multipart(
        f"https://{server}.gofile.io/contents/uploadfile",
        {},
        "file",
        path,
    )
    res = json.loads(raw)
    if res.get("status") != "ok":
        raise RuntimeError(f"gofile error: {raw[:160]}")
    data = res["data"]
    return {
        "url": data["downloadPage"],
        "expira": "inactividad (~10d) o DELETE /api/archivo",
        "host": "gofile",
        "gofile_id": data.get("id") or data.get("parentFolder"),
        "gofile_token": data.get("guestToken", ""),
    }


def main() -> int:
    work = tempfile.mkdtemp(prefix="hlsjob_")
    estado = {
        "id": ISSUE,
        "estado": "descargando",
        "progreso": 0,
        "nota": "iniciando",
    }
    update_issue(estado)
    try:
        consulta = os.environ["CONSULTA"]
        tipo = (os.environ.get("TIPO") or "single").strip().lower()
        grupo = os.environ.get("GRUPO") or "1"
        parte = os.environ.get("PARTE") or "1"
        calidad = os.environ.get("CALIDAD") or "720p"
        anio = (os.environ.get("ANIO") or "").strip()

        args = ["hlsync", consulta, "-o", work, "-j", "32", "--pick", "0", "-q", calidad]
        if tipo == "multi":
            args += ["--multi", "-s", grupo, "-e", parte]
        else:
            args += ["--single"]
        if anio:
            args += ["--year", anio]

        proc = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=work,
        )
        last_pct = -1
        for linea in proc.stdout:
            linea = linea.strip()
            print(linea, flush=True)
            m = PROGRESO.search(linea)
            if m:
                pct = int(m.group(3))
                estado.update(progreso=pct, nota=linea)
                if pct - last_pct >= 15 or pct == 100:
                    last_pct = pct
                    try:
                        update_issue(estado)
                    except Exception:
                        pass
            elif linea[:2] in (">>", "OK", "!!") or linea.startswith("!"):
                estado["nota"] = linea
        proc.wait()

        mp4s = [f for f in os.listdir(work) if f.endswith(".mp4")]
        if proc.returncode != 0 or not mp4s:
            estado["estado"] = "error"
            estado["nota"] = estado.get("nota") or f"exit={proc.returncode}"
            update_issue(estado, close=True)
            return 1

        out_file = os.path.join(work, mp4s[0])
        size = os.path.getsize(out_file)
        estado.update(nombre=mp4s[0], tamano=size, nota="publicando", progreso=99)
        update_issue(estado)

        if size <= LITTERBOX_MAX:
            try:
                pub = upload_litterbox(out_file)
            except Exception:
                pub = upload_gofile(out_file)
        else:
            pub = upload_gofile(out_file)

        estado.update(
            estado="listo",
            progreso=100,
            nota="disponible",
            **pub,
        )
        update_issue(estado)
        return 0
    except Exception as exc:  # noqa: BLE001
        estado["estado"] = "error"
        estado["nota"] = str(exc)
        update_issue(estado, close=True)
        return 1
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
