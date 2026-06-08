import contextlib
import io
import logging
from pathlib import Path
from threading import Lock
from typing import Any, Callable


OUTPUT_FILE_PATH = Path(r"D:\\raggy_logs\\massive_stream_output.log")

_OUTPUT_FILE_LOCK = Lock()


def append_output_to_file(text: str) -> None:
    OUTPUT_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)

    with _OUTPUT_FILE_LOCK:
        with OUTPUT_FILE_PATH.open("a", encoding="utf-8") as file:
            file.write(text)
            if not text.endswith("\n"):
                file.write("\n")
            file.flush()


def run_stdout_to_file(fn: Callable[[], Any]) -> Any:
    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()

    with contextlib.redirect_stdout(stdout_buffer), contextlib.redirect_stderr(stderr_buffer):
        result = fn()

    stdout_text = stdout_buffer.getvalue()
    stderr_text = stderr_buffer.getvalue()

    if stdout_text:
        append_output_to_file(stdout_text)

    if stderr_text:
        append_output_to_file(stderr_text)

    return result


def configure_logging() -> None:
    OUTPUT_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        filename=str(OUTPUT_FILE_PATH),
        filemode="a",
        encoding="utf-8",
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )