import pandas as pd
import time
import os

class PipelineStep:
    def __init__(self, name:str, validator):
        self.removed_rows = pd.DataFrame()
        self.name = name
        self.start_time = None
        self.end_time = None
        self.stats = {} # reporting metrics
        self.validator = validator

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """Override this method in subclasses"""
        raise NotImplementedError
    
    def run_with_timer(self, df):
        self.start_time = time.time()
        df_result = self.run(df)
        self.end_time = time.time()
        self.stats['runtime_sec'] = self.end_time - self.start_time
        return df_result

class Pipeline:
    def __init__(self, steps, tokeniser_step):
        self.steps = steps
        self.tokeniser_step = tokeniser_step
        self.removed_rows = 0
        self.originaldf_length = 0

    def run(self, df: pd.DataFrame):
        self.originaldf_length = len(df)
        for step in self.steps:
            print(f"Running step: {step.name}")
            df = step.run_with_timer(df)
            # validate
            step.validator.validate(df)
            print(step.validator.stats)
            print(f"Number of rows dropped: {len(step.removed_rows)}")
            self.removed_rows = self.removed_rows + len(step.removed_rows)
            


            # store dropped rows
            report_dir = "../../reports"
            os.makedirs(report_dir, exist_ok=True)  # Ensure the directory exists

            # Store dropped rows
            if hasattr(step, "removed_rows") and len(step.removed_rows) > 0:
                dropped_file = os.path.join(report_dir, f"dropped_{step.name}.csv")
                step.removed_rows.to_csv(dropped_file, index=False)

        for step in self.tokeniser_step:
            tokeniser_df = step.run_with_timer(df)
            print(f"Tokeniser run")

        # reconcile dropped records and current df to original
        dropedpluscurrent = self.removed_rows + len(df)
        print(f"Rows dropped: {self.removed_rows}, Rows kept: {len(df)}")
        print(f"Total: {dropedpluscurrent} Original: {self.originaldf_length}")
        return df, tokeniser_df