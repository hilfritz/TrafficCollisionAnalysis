from datetime import datetime
from pathlib import Path
from time import perf_counter


LOG_FILE = Path("logs/app_benchmark.log")


def reset_log(log_file: Path = LOG_FILE) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    log_file.write_text("", encoding="utf-8")


def log_message(message: str, log_file: Path = LOG_FILE) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def log_timed_block(name: str, log_file: Path = LOG_FILE):
    log_message(f"{name} START", log_file=log_file)
    start = perf_counter()

    def end():
        elapsed = perf_counter() - start
        elapsed_ms = elapsed * 1000
        log_message(
            f"{name} END took {elapsed:.6f}s ({elapsed_ms:.2f} ms)",
            log_file=log_file,
        )
        return elapsed

    return end


def benchmark_call(timings: list[dict], func_name: str, func, *args, **kwargs):
    end_log = log_timed_block(func_name)
    result = func(*args, **kwargs)
    elapsed = end_log()
    timings.append(
        {
            "Function": func_name,
            "Execution Time (s)": round(elapsed, 6),
            "Execution Time (ms)": round(elapsed * 1000, 2),
        }
    )
    return result