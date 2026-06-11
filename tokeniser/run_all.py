import subprocess
import signal
import time

processes = []

scripts = [
    "tokeniser/visa_tokeniser.py",
    "tokeniser/mastercard_tokeniser.py",
    "tokeniser/mpesa_tokeniser.py",
    "tokeniser/pesapal_tokeniser.py",
    "tokeniser/flutter_tokeniser.py"
]


def shutdown(signum=None, frame=None):
    print("\nStopping all producers...")

    for process in processes:
        process.terminate()

    for process in processes:
        process.wait()

    print("All producers stopped.")
    exit(0)


if __name__ == "__main__":

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print("Starting all raw topic producers...\n")

    for script in scripts:
        process = subprocess.Popen(["python", script])
        processes.append(process)
        print(f"Started {script}")

    try:
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        shutdown()