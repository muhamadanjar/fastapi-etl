import time
import os

class ETLService:
    def __init__(self, repo):
        self.repo = repo  # Repository to save results

    def run(self, source: str):
        # Simulate processing time (replace with real ETL logic)
        time.sleep(5)
        result = f"processed: {source}"
        # Save result to database
        self.repo.save_result(source, result)

    def run_with_file(self, file_path: str):
        filename = os.path.basename(file_path)
        # Simulate reading a large file and counting lines
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = sum(1 for _ in f)
        result = f"processed file {filename} with {lines} lines"
        # Save result to database
        self.repo.save_result(filename, result)
        # Clean up the uploaded file after processing
        os.remove(file_path)