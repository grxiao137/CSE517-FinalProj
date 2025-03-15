# from .task import BirdBench
try:
    from .task import BirdBench
except Exception as e:
    print("Error during import of BirdBench:", e)
    raise  # re-raise so you see the full traceback
# from .baseline_task import BirdBench
