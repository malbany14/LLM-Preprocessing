import pandas as pd
from pipeline import PipelineStep
from utils import flag_toxic_keywords
from utils import mask_text

class PiiRemovalStep(PipelineStep):
    def __init__(self, name, validator=None):
        super().__init__(name, validator)

    def run(self, df):
        df[['text', 'masked_items']] = df['text'].apply(lambda x: pd.Series(mask_text(x)))
        return df

class ToxicRemovalStep(PipelineStep):
    def __init__(self, name, validator=None):
        super().__init__(name, validator)
        self.removed_rows = pd.DataFrame()    # Store rows removed due to toxicity

    def run(self, df):
        """
        Remove rows based on keyword filtering
        """

        df['is_inappropriate'] = df['text'].apply(flag_toxic_keywords)

        self.removed_rows = df[df["is_inappropriate"] == True] # set toxic rows in self.removed_rows

        df = df[df["is_inappropriate"] == False]
        return df