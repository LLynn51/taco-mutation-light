"""Chat-completions transport, streaming or plain JSON. No hidden retries."""

import http.client
import json
import os
import stat
import time
import threading
import socket
from pathlib import Path
from urllib.parse import urlsplit


class RequestFailure(Exception):
    def __init__(self, detail):
        self.detail = detail
        super().__init__(detail.get("error", "request_failed"))


def credential(config):
    path = Path(config["credential_file"])
    if not config.get("credential_file"):
        raise ValueError("Set api.credential_file explicitly")
    info = path.stat()
    if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) not in (0o400, 0o600):
        raise ValueError("Credential file must be user-owned with mode 0400 or 0600")
    values = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        if sep:
            values[key.strip()] = value.strip().strip("\"'")
    key = config.get("key_name", "DEEPSEEK_API_KEY")
    if key not in values or not values[key]:
        raise ValueError("Credential key missing")
    # No environment expansion, shell sourcing, global environment changes or key logging.
    if os.environ.get(key) and os.environ[key] != values[key]:
        raise ValueError("Existing credential environment differs")
    return values[key]


def payload(config, prompt, material):
    if config.get("protocol", "chat_completions") != "chat_completions":
        raise ValueError("Unsupported api.protocol; implement it in llm.py")
    body = {
        "model": config["model"],
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": json.dumps(material, ensure_ascii=False)},
        ],
        "stream": config.get("stream", True),
    }
    if config.get("max_tokens"):
        body["max_tokens"] = config["max_tokens"]
    if config.get("reasoning_effort"):
        body["reasoning_effort"] = config["reasoning_effort"]
    if config.get("thinking"):
        body["thinking"] = {"type": config["thinking"]}
    if config.get("json_object", True):
        body["response_format"] = {"type": "json_object"}
    if body["stream"]:
        body["stream_options"] = {"include_usage": True}
    body.update(config.get("extra", {}))
    return body


class Client:
    def __init__(self, config, key_loader=credential):
        self.config = config
        self.key_loader = key_loader

    def complete(self, body):
        cfg = self.config
        url = urlsplit(cfg["base_url"])
        if url.scheme != "https" and not (
            url.scheme == "http" and url.hostname in ("127.0.0.1", "localhost", "::1")
        ):
            raise ValueError("HTTPS required outside localhost")
        if url.username or url.password or url.query or url.fragment:
            raise ValueError("base_url must not contain credentials/query/fragment")
        conn = None
        timer = None
        start = time.monotonic()
        detail = {
            "stage": "credential",
            "http_status": None,
            "usage": None,
            "finish_reason": None,
        }
        try:
            key = self.key_loader(cfg)
            cls = (
                http.client.HTTPSConnection
                if url.scheme == "https"
                else http.client.HTTPConnection
            )
            conn = cls(url.hostname, url.port, timeout=cfg.get("connect_timeout", 30))
            detail["stage"] = "connect"
            conn.connect()

            def abort():
                try:
                    if conn.sock:
                        conn.sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

            timer = threading.Timer(cfg.get("total_timeout", 900), abort)
            timer.daemon = True
            timer.start()
            conn.sock.settimeout(cfg.get("read_timeout", 180))
            path = url.path.rstrip("/")
            if not path.endswith("/chat/completions"):
                path += "/chat/completions"
            detail["stage"] = "send"
            conn.request(
                "POST",
                path,
                body=json.dumps(body).encode(),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": "Bearer " + key,
                },
            )
            key = None
            detail["stage"] = "headers"
            response = conn.getresponse()
            detail["http_status"] = response.status
            if response.status != 200:
                raise RequestFailure({**detail, "error": "http_error"})
            detail["stage"] = "response"
            content = []
            reasoning_chars = 0
            usage = None
            finish = None
            done = False
            if body.get("stream"):
                while True:
                    remaining = cfg.get("total_timeout", 900) - (
                        time.monotonic() - start
                    )
                    if remaining <= 0:
                        raise TimeoutError()
                    if conn.sock:
                        conn.sock.settimeout(
                            min(cfg.get("read_timeout", 180), remaining)
                        )
                    line = response.readline()
                    if not line:
                        break
                    if not line.startswith(b"data:"):
                        continue
                    raw = line[5:].strip()
                    if raw == b"[DONE]":
                        done = True
                        break
                    obj = json.loads(raw)
                    if obj.get("usage") is not None:
                        usage = obj["usage"]
                        detail["usage"] = usage
                    for choice in obj.get("choices", []):
                        if choice.get("index", 0) != 0:
                            continue
                        delta = choice.get("delta", {})
                        content.append(delta.get("content") or "")
                        reasoning_chars += len(delta.get("reasoning_content") or "")
                        if choice.get("finish_reason") is not None:
                            finish = choice["finish_reason"]
            else:
                chunks = []
                while True:
                    remaining = cfg.get("total_timeout", 900) - (
                        time.monotonic() - start
                    )
                    if remaining <= 0:
                        raise TimeoutError()
                    if conn.sock:
                        conn.sock.settimeout(
                            min(cfg.get("read_timeout", 180), remaining)
                        )
                    chunk = response.read1(65536)
                    if not chunk:
                        break
                    chunks.append(chunk)
                obj = json.loads(b"".join(chunks))
                choice = obj["choices"][0]
                usage = obj.get("usage")
                finish = choice.get("finish_reason")
                content = [choice["message"].get("content") or ""]
                reasoning_chars = len(choice["message"].get("reasoning_content") or "")
                done = True
            detail.update(
                usage=usage, finish_reason=finish, reasoning_chars=reasoning_chars
            )
            if finish != "stop" or not done:
                raise RequestFailure({**detail, "error": "unfinished_response"})
            text = "".join(content).strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
            detail["stage"] = "parse"
            parsed = json.loads(text)
            if not isinstance(parsed, dict):
                raise ValueError()
            return {
                "response": parsed,
                "usage": usage,
                "finish_reason": finish,
                "reasoning_chars": reasoning_chars,
                "elapsed_seconds": time.monotonic() - start,
                "http_status": response.status,
            }
        except Exception as exc:
            failure = (
                exc.detail
                if isinstance(exc, RequestFailure)
                else {**detail, "error": type(exc).__name__}
            )
            if conn is not None:
                conn.close()
            failure["local_connection_closed"] = True
            failure.update(
                elapsed_seconds=time.monotonic() - start,
                remote_result="unknown"
                if detail["stage"] in ("send", "headers", "response")
                else "received_or_not_sent",
            )
            raise RequestFailure(failure) from None
        finally:
            if timer is not None:
                timer.cancel()
            if conn is not None:
                conn.close()
